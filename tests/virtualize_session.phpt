--TEST--
virtualize(): each request starts with a fresh session state and ends by writing and closing its session (the Yii, Laminas and CodeIgniter leaks)
--EXTENSIONS--
phasync
session
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--INI--
session.use_strict_mode=0
session.use_cookies=1
session.use_only_cookies=1
session.cache_limiter=nocache
--FILE--
<?php
require __DIR__ . '/sink.inc';
use function phasync\ext\virtualize;

$dir = sys_get_temp_dir() . '/phasync_sess_' . getmypid();
@mkdir($dir);
ini_set('session.save_path', $dir);

function cookieOf(Sink $s): ?string {
    foreach ($s->headers[2] ?? [] as $h) {
        if (preg_match('/^Set-Cookie: PHPSESSID=([^;]+)/', $h, $m)) return $m[1];
    }
    return null;
}

// A visitor logs in; the request ends and its session is written.
$_COOKIE = [];
$s = new Sink;
virtualize(function () { session_start(); $_SESSION['user'] = 'alice'; }, $s);
$alice = cookieOf($s);
var_dump(strlen($alice) > 0, session_status() === PHP_SESSION_NONE);   // the worker's own: untouched

// Yii: after session_id(''), the next visitor without a cookie got the previous
// visitor's session. Every request now starts with none.
$_COOKIE = [];
$s = new Sink;
virtualize(function () { session_id(''); }, $s);
$_COOKIE = [];
$s = new Sink;
$seen = null;
virtualize(function () use (&$seen) {
    $seen = [session_status(), session_id()];
    session_start();
    $seen[] = $_SESSION;
}, $s);
var_dump($seen, cookieOf($s) !== $alice);

// Laminas: no reset trick needed; a request that starts a session and then
// ends without closing it doesn't leave it open for the next.
$_COOKIE = [];
virtualize(function () { session_start(); $_SESSION['left'] = 'open'; }, new Sink);
virtualize(function () use (&$seen) { $seen = [session_status(), session_id()]; }, new Sink);
var_dump($seen);

// CodeIgniter: the id from the request's cookie still finds the session.
$_COOKIE = ['PHPSESSID' => $alice];
virtualize(function () use (&$seen) { session_start(); $seen = [session_id(), $_SESSION]; }, $s = new Sink);
var_dump($seen[0] === $alice, $seen[1]);

// Two requests interleaved, each with its own session. $_SESSION is a global
// variable, which virtualize() leaves alone: phasync swaps globals per request,
// keeping $_SESSION the reference ext/session holds; here each request keeps its own.
$a = new Sink; $b = new Sink;
$_COOKIE = ['PHPSESSID' => $alice];
$fa = new Fiber(fn() => virtualize(function () {
    session_start();
    $session = &$_SESSION;
    Fiber::suspend();
    $session['visits'] = ($session['visits'] ?? 0) + 1;
    echo session_id() === $GLOBALS['alice'] ? "a: alice's session" : "a: WRONG session";
}, $a));
$fa->start();
$_COOKIE = [];
$fb = new Fiber(fn() => virtualize(function () {
    session_start();
    Fiber::suspend();
    echo session_id() !== $GLOBALS['alice'] ? "b: a new session" : "b: WRONG session";
}, $b));
$fb->start();
$fa->resume(); $fb->resume();
echo $a->out, "\n", $b->out, "\n";
$_COOKIE = ['PHPSESSID' => $alice];
virtualize(function () use (&$seen) { session_start(); $seen = $_SESSION; }, new Sink);
var_dump($seen);

// A request's own save handler is its own; the next request uses the worker's.
class MemoryHandler implements SessionHandlerInterface {
    public static array $data = [];
    public function open(string $p, string $n): bool { return true; }
    public function close(): bool { return true; }
    public function read(string $id): string|false { return self::$data[$id] ?? ''; }
    public function write(string $id, string $d): bool { self::$data[$id] = $d; return true; }
    public function destroy(string $id): bool { unset(self::$data[$id]); return true; }
    public function gc(int $max): int|false { return 0; }
}
$_COOKIE = [];
virtualize(function () { session_set_save_handler(new MemoryHandler); session_start(); $_SESSION['in'] = 'memory'; }, new Sink);
var_dump(count(MemoryHandler::$data));
$_COOKIE = ['PHPSESSID' => $alice];
virtualize(function () use (&$seen) { session_start(); $seen = $_SESSION['user'] ?? null; }, new Sink);
var_dump($seen);

array_map('unlink', glob("$dir/sess_*"));
rmdir($dir);
?>
--EXPECT--
bool(true)
bool(true)
array(3) {
  [0]=>
  int(1)
  [1]=>
  string(0) ""
  [2]=>
  array(0) {
  }
}
bool(true)
array(2) {
  [0]=>
  int(1)
  [1]=>
  string(0) ""
}
bool(true)
array(1) {
  ["user"]=>
  string(5) "alice"
}
a: alice's session
b: a new session
array(2) {
  ["user"]=>
  string(5) "alice"
  ["visits"]=>
  int(1)
}
int(1)
string(5) "alice"
