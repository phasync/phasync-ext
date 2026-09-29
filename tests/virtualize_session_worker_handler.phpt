--TEST--
virtualize(): a save handler the worker set with session_set_save_handler() is each request's (#15)
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

class MemoryHandler implements SessionHandlerInterface {
    public array $data = [];
    public array $calls = [];
    public function open(string $p, string $n): bool { $this->calls[] = 'open'; return true; }
    public function close(): bool { $this->calls[] = 'close'; return true; }
    public function read(string $id): string|false { $this->calls[] = "read $id"; return $this->data[$id] ?? ''; }
    public function write(string $id, string $d): bool { $this->calls[] = "write $id"; $this->data[$id] = $d; return true; }
    public function destroy(string $id): bool { unset($this->data[$id]); return true; }
    public function gc(int $max): int|false { return 0; }
}

// The worker sets its handler once, and keeps no other reference to it.
$h = new MemoryHandler;
$weak = WeakReference::create($h);
session_set_save_handler($h, false);
unset($h);

// Two interleaved requests, each on its own session, both through the worker's handler.
$a = Sink::withSession('ha'); $b = Sink::withSession('hb');
$fa = new Fiber(fn() => virtualize(function () {
    session_start();
    $_SESSION['who'] = 'a';
    Fiber::suspend();
}, $a));
$fb = new Fiber(fn() => virtualize(function () {
    session_start();
    $_SESSION['who'] = 'b';
    Fiber::suspend();
}, $b));
$fa->start(); $fb->start(); $fa->resume(); $fb->resume();
echo $a->out, $b->out;

// A request that sets its own handler replaces it for itself only.
virtualize(function () { session_set_save_handler(new MemoryHandler, false); session_start(); $_SESSION['own'] = 1; }, Sink::withSession('own'));

// The worker's handler got both requests' sessions, and still serves the next one.
$seen = null;
virtualize(function () use (&$seen) { session_start(); $seen = $_SESSION; }, Sink::withSession('ha'));
var_dump($seen);
virtualize(function () use (&$seen) { session_start(); $seen = $_SESSION; }, Sink::withSession('own'));
var_dump($seen);

// Each request held its own reference: the worker's handler is still there, with both
// sessions (and the empty one the last request wrote; the own handler's data isn't here).
var_dump($weak->get()?->data);
?>
--EXPECT--
array(1) {
  ["who"]=>
  string(1) "a"
}
array(0) {
}
array(3) {
  ["ha"]=>
  string(12) "who|s:1:"a";"
  ["hb"]=>
  string(12) "who|s:1:"b";"
  ["own"]=>
  string(0) ""
}
