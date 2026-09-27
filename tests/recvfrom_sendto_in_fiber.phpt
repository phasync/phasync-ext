--TEST--
stream_socket_recvfrom()/stream_socket_sendto() park the coroutine inside a scope (udp, udg, tcp) and keep native results
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';

// $op runs in a coroutine beside a ticker; $helper makes it ready after 200ms.
function coop(string $name, Closure $op, Closure $helper): mixed {
    $ticks = 0; $done = false; $res = null;
    (new Loop)->runAll(
        function () use ($op, &$done, &$res) { $res = $op(); $done = true; },
        $helper,
        function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } },
    );
    printf("%-26s %s\n", $name, $ticks >= 5 ? 'cooperative' : "BLOCKED (ticks=$ticks)");
    return $res;
}

// UDP, the server created inside the scope (so wrapped by the udp transport).
$res = null;
(new Loop)->runAll(function () use (&$res) {
    $srv = stream_socket_server('udp://127.0.0.1:0', $e, $es, STREAM_SERVER_BIND);
    $addr = stream_socket_get_name($srv, false);
    $cli = stream_socket_client("udp://$addr");
    $res = [$srv, $cli];
});
[$srv, $cli] = $res;
$cliName = stream_socket_get_name($cli, false);
$got = coop('udp recvfrom', function () use ($srv) {
    $data = stream_socket_recvfrom($srv, 100, 0, $from);
    return [$data, $from];
}, function () use ($cli) { usleep(200000); stream_socket_sendto($cli, 'hello'); });
var_dump($got === ['hello', $cliName]);

// Native parity with the datagram already there: a peek, then the read.
$peekRead = fn() => [stream_socket_recvfrom($srv, 100, STREAM_PEEK, $f1), $f1, stream_socket_recvfrom($srv, 100, 0, $f2), $f2];
stream_socket_sendto($cli, 'one');
$native = $peekRead();
stream_socket_sendto($cli, 'one');
$ext = null;
(new Loop)->runAll(function () use ($peekRead, &$ext) { $ext = $peekRead(); });
var_dump($native === ['one', $cliName, 'one', $cliName], $ext === $native);

// Unix datagram sockets.
$path = sys_get_temp_dir() . '/phasync_udg_' . getmypid() . '.sock';
@unlink($path);
$usrv = stream_socket_server("udg://$path", $e, $es, STREAM_SERVER_BIND);
$ucli = stream_socket_client("udg://$path");
$got = coop('udg recvfrom', fn() => stream_socket_recvfrom($usrv, 100),
    function () use ($ucli) { usleep(200000); stream_socket_sendto($ucli, 'unix'); });
var_dump($got);
@unlink($path);

// TCP: recvfrom waits for data; sendto waits while the send buffer is full.
$tsrv = stream_socket_server('tcp://127.0.0.1:0');
$tcli = stream_socket_client('tcp://' . stream_socket_get_name($tsrv, false));
$tacc = stream_socket_accept($tsrv);
$got = coop('tcp recvfrom', fn() => stream_socket_recvfrom($tacc, 100),
    function () use ($tcli) { usleep(200000); fwrite($tcli, 'tcp'); });
var_dump($got);
stream_set_blocking($tcli, false);
while (fwrite($tcli, str_repeat('x', 65536)) > 0) {}   // fill the send buffer
stream_set_blocking($tcli, true);
$got = coop('tcp sendto', fn() => stream_socket_sendto($tcli, 'y'),
    function () use ($tacc) { usleep(200000); stream_set_blocking($tacc, false); while (fread($tacc, 65536) !== '') {} });
var_dump($got);

// A non-blocking stream stays native: nothing to receive, no wait.
stream_set_blocking($srv, false);
$l = new Loop;
$r = null;
$l->runAll(function () use ($srv, &$r) { $r = @stream_socket_recvfrom($srv, 100); });
var_dump($r, $l->parks);
?>
--EXPECT--
udp recvfrom               cooperative
bool(true)
bool(true)
bool(true)
udg recvfrom               cooperative
string(4) "unix"
tcp recvfrom               cooperative
string(3) "tcp"
tcp sendto                 cooperative
int(1)
bool(false)
int(0)
