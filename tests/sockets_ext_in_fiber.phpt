--TEST--
ext/sockets: socket_read/recv/recvfrom/write/send/sendto/accept/connect park the coroutine inside a scope and keep native results, errors and SO_RCVTIMEO timeouts
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
// The body runs in a child PHP with the normal ini; skip if that has no ext/sockets.
if (trim((string) shell_exec(escapeshellarg(PHP_BINARY) . ' -r "echo function_exists(\'socket_create\') ? 1 : 0;"')) !== '1') die('skip requires ext/sockets');
?>
--FILE--
<?php
// ext/sockets is usually a shared extension that run-tests' `-n` can't load, so
// the body runs in a child PHP with the normal ini and the extension loaded.
$child = tempnam(sys_get_temp_dir(), 'phasync_sx_') . '.php';
file_put_contents($child, '<?php require ' . var_export(__DIR__ . '/loop.inc', true) . ';' . <<<'CHILD'

// $op runs in a coroutine beside a ticker; $helper makes it ready after 200ms.
function coop(string $name, Closure $op, Closure $helper): mixed {
    $ticks = 0; $done = false; $res = null;
    (new Loop)->runAll(
        function () use ($op, &$done, &$res) { $res = $op(); $done = true; },
        $helper,
        function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } },
    );
    printf("%-22s %s\n", $name, $ticks >= 5 ? 'cooperative' : "BLOCKED (ticks=$ticks)");
    return $res;
}

socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $p);
var_dump(coop('socket_read', fn() => socket_read($p[0], 100), function () use ($p) { usleep(200000); socket_write($p[1], 'read'); }));
var_dump(coop('socket_recv', function () use ($p) { $n = socket_recv($p[0], $buf, 100, 0); return [$n, $buf]; },
    function () use ($p) { usleep(200000); socket_send($p[1], 'recv', 4, 0); }));

$u = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
socket_bind($u, '127.0.0.1', 0);
socket_getsockname($u, $uaddr, $uport);
$uc = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
socket_bind($uc, '127.0.0.1', 0);
socket_getsockname($uc, $caddr, $cport);
$r = coop('socket_recvfrom', function () use ($u) { $n = socket_recvfrom($u, $buf, 100, 0, $from, $port); return [$n, $buf, $from, $port]; },
    function () use ($uc, $uaddr, $uport) { usleep(200000); socket_sendto($uc, 'udp', 3, 0, $uaddr, $uport); });
var_dump($r === [3, 'udp', '127.0.0.1', $cport]);

// A full send buffer: socket_write waits until the peer drains it.
socket_set_nonblock($p[0]);
while (@socket_write($p[0], str_repeat('x', 65536)) > 0) {}
socket_set_block($p[0]);
var_dump(coop('socket_write (full)', fn() => socket_write($p[0], 'y'), function () use ($p) {
    usleep(200000); socket_set_nonblock($p[1]); while (@socket_read($p[1], 65536) !== false) {} socket_set_block($p[1]); }));
socket_set_nonblock($p[1]); while (@socket_read($p[1], 65536) !== false) {} socket_set_block($p[1]);

// accept and connect.
$l = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
socket_bind($l, '127.0.0.1', 0); socket_listen($l); socket_getsockname($l, $laddr, $lport);
$c = coop('socket_accept', fn() => socket_accept($l), function () use ($lport) {
    usleep(200000); $cl = socket_create(AF_INET, SOCK_STREAM, SOL_TCP); socket_connect($cl, '127.0.0.1', $lport); $GLOBALS['cl'] = $cl; });
var_dump($c instanceof Socket);
$r = null;
(new Loop)->runAll(function () use ($lport, &$r) { $s = socket_create(AF_INET, SOCK_STREAM, SOL_TCP); $r = socket_connect($s, '127.0.0.1', $lport); });
var_dump($r);

// connect refused: the native warning and socket error.
$refused = function () {
    $s = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
    error_clear_last();
    $r = @socket_connect($s, '127.0.0.1', 1);
    return [$r, preg_replace('/^.*?\(\): /', '', error_get_last()['message'] ?? ''), socket_last_error($s), socket_last_error()];
};
$native = $refused();
$ext = null;
(new Loop)->runAll(function () use ($refused, &$ext) { $ext = $refused(); });
echo 'connect refused: ', $native === $ext ? 'same' : 'DIFF ' . json_encode([$native, $ext]), ' ', $ext[1], "\n";

// A unix path.
$path = sys_get_temp_dir() . '/phasync_sx_' . getmypid() . '.sock';
@unlink($path);
$ul = socket_create(AF_UNIX, SOCK_STREAM, 0); socket_bind($ul, $path); socket_listen($ul);
(new Loop)->runAll(function () use ($path, &$r) { $s = socket_create(AF_UNIX, SOCK_STREAM, 0); $r = socket_connect($s, $path); });
var_dump($r);
@unlink($path);

// SO_RCVTIMEO: times out like native (false, the warning, EAGAIN).
socket_set_option($p[0], SOL_SOCKET, SO_RCVTIMEO, ['sec' => 0, 'usec' => 200000]);
$timed = function () use ($p) {
    error_clear_last();
    $r = @socket_read($p[0], 10);
    return [$r, preg_replace('/^.*?\(\): /', '', error_get_last()['message'] ?? ''), socket_last_error($p[0])];
};
$native = $timed();
$loop = new Loop;
$loop->runAll(function () use ($timed, &$ext) { $ext = $timed(); });
echo 'SO_RCVTIMEO: ', $native === $ext ? 'same' : 'DIFF ' . json_encode([$native, $ext]), ', parked with ', round($loop->parkTimeouts[0], 1), " s\n";
socket_set_option($p[0], SOL_SOCKET, SO_RCVTIMEO, ['sec' => 0, 'usec' => 0]);

// A non-blocking socket stays native: no park.
socket_set_nonblock($p[0]);
$loop = new Loop;
$loop->runAll(fn() => @socket_read($p[0], 10));
echo 'non-blocking parks: ', $loop->parks, "\n";
socket_set_block($p[0]);

// A cancelled read consumes nothing.
$loop = new Loop;
$loop->runAll(
    function () use ($p) {
        $GLOBALS['w'] = Fiber::getCurrent();
        try { socket_read($p[0], 10); echo "read?!\n"; } catch (LoopCancelled $e) { echo "read cancelled\n"; }
    },
    function () use ($loop, $p) { usleep(50000); $loop->cancel($GLOBALS['w']); },
);
socket_write($p[1], 'kept');
var_dump(socket_read($p[0], 10));
CHILD);
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' ' . escapeshellarg($child) . ' 2>&1');
unlink($child);
?>
--EXPECT--
socket_read            cooperative
string(4) "read"
socket_recv            cooperative
array(2) {
  [0]=>
  int(4)
  [1]=>
  string(4) "recv"
}
socket_recvfrom        cooperative
bool(true)
socket_write (full)    cooperative
int(1)
socket_accept          cooperative
bool(true)
bool(true)
connect refused: same unable to connect [111]: Connection refused
bool(true)
SO_RCVTIMEO: same, parked with 0.2 s
non-blocking parks: 0
read cancelled
string(4) "kept"
