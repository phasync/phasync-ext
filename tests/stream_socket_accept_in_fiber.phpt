--TEST--
stream_socket_accept() with a timeout suspends the fiber inside a scope; timeouts and timeout 0 behave natively
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
final class TestTimeout extends Exception {}
$srv  = stream_socket_server('tcp://127.0.0.1:0');
$addr = stream_socket_get_name($srv, false);

// 1. Fiber A waits in accept; the scope's driver (here: inline) connects meanwhile.
$log = [];
\phasync\ext\manage(function () use ($srv, $addr, &$log) {
    $a = new Fiber(function () use ($srv, &$log) {
        $c = stream_socket_accept($srv, 5, $peer);   // nothing pending yet -> suspends
        $log[] = 'A accepted: ' . (is_resource($c) ? 'yes' : 'no') . ', peer is ' . (str_starts_with($peer, '127.0.0.1:') ? 'set' : var_export($peer, true));
    });
    $res = $a->start();
    $log[] = 'A parked on the listener: ' . var_export($res === $srv, true);
    $cli = stream_socket_client("tcp://$addr");       // make the listener readable
    $r = [$res]; $w = $e = null;
    \phasync\ext\stream_select($r, $w, $e, 2);
    $a->resume();
}, fn($s, $t) => Fiber::suspend($s), fn($s, $t) => Fiber::suspend($s), fn($us) => null, TestTimeout::class);
echo implode("\n", $log), "\n";

// 2. Timeout: native warning and false (handler waits like phasync, then times out).
$waitReal = function ($s, $t) {
    $r = [$s]; $w = $e = null;
    if (\phasync\ext\stream_select($r, $w, $e, 0, (int) ($t * 1e6)) < 1) throw new TestTimeout();
};
$called = 0;
(new Fiber(fn() => \phasync\ext\manage(function () use ($srv) {
    var_dump(stream_socket_accept($srv, 0.1));
    var_dump(stream_socket_accept($srv, 0));        // don't-wait: straight to the original
}, function ($s, $t) use ($waitReal, &$called) { $called++; $waitReal($s, $t); },
   $waitReal, fn($us) => null, TestTimeout::class)))->start();
?>
--EXPECTF--
A parked on the listener: true
A accepted: yes, peer is set

Warning: stream_socket_accept(): Accept failed: %s timed out in %s on line %d
bool(false)

Warning: stream_socket_accept(): Accept failed: %s timed out in %s on line %d
bool(false)
