--TEST--
Transparent async: fread on a hooked socket parks the coroutine until data arrives
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
$server = stream_socket_server("tcp://127.0.0.1:0", $errno, $errstr);
$addr = stream_socket_get_name($server, false);
$loop->runAll(
    function () use ($addr, $loop) {
        $conn = stream_socket_client("tcp://$addr", $e, $es, 1);
        echo "fiber read: " . fread($conn, 100) . "\n";   // no data yet -> parks
    },
    function () use ($server) {
        $sconn = stream_socket_accept($server, 1);
        usleep(100000);
        echo "writing\n";
        fwrite($sconn, "hello");
    },
);
var_dump($loop->parks > 0);
?>
--EXPECT--
writing
fiber read: hello
bool(true)
