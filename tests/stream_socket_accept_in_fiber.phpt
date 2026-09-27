--TEST--
stream_socket_accept() with a timeout parks the coroutine inside a scope; timeouts match native
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$srv  = stream_socket_server('tcp://127.0.0.1:0');
$addr = stream_socket_get_name($srv, false);

// 1. A waits in accept; B connects meanwhile.
$log = [];
$loop = new Loop;
$loop->runAll(
    function () use ($srv, &$log) {
        $c = stream_socket_accept($srv, 5, $peer);   // nothing pending yet -> parks
        $log[] = 'A accepted: ' . (is_resource($c) ? 'yes' : 'no') . ', peer is ' . (str_starts_with($peer, '127.0.0.1:') ? 'set' : var_export($peer, true));
    },
    function () use ($addr, &$log) {
        usleep(50000);
        $log[] = 'B connects';
        $GLOBALS['cli'] = stream_socket_client("tcp://$addr");
    },
);
echo implode("\n", $log), "\n";
var_dump(round($loop->parkTimeouts[0], 2));          // float(5): accept's own timeout

// 2. Timeout: native warning and false; timeout 0 means don't wait (native).
$loop = new Loop;
$loop->runAll(function () use ($srv) {
    var_dump(stream_socket_accept($srv, 0.1));
    var_dump(stream_socket_accept($srv, 0));
});
var_dump($loop->parks);                             // int(1): only the 0.1 s one waited
?>
--EXPECTF--
B connects
A accepted: yes, peer is set
float(5)

Warning: stream_socket_accept(): Accept failed: %s timed out in %s on line %d
bool(false)

Warning: stream_socket_accept(): Accept failed: %s timed out in %s on line %d
bool(false)
int(1)
