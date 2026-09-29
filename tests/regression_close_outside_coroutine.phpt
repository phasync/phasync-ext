--TEST--
Regression: closing a stream outside a coroutine while a coroutine is suspended in an operation on it is a fatal error, not a use-after-free (#14)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
?>
--FILE--
<?php
// Only a coroutine can wait for the suspended one to leave the operation; closing
// from the loop itself would free the stream under it.
require __DIR__ . '/loop.inc';
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
$loop = new Loop;
$loop->manage(function () use ($a) {
    $reader = new Fiber(function () use ($a) { fread($a, 100); echo "not reached\n"; });
    $reader->start();
    echo "reader suspended\n";
    fclose($a);
});
?>
--EXPECTF--
reader suspended

Fatal error: phasync: a stream was closed outside a coroutine while 1 coroutine(s) wait on it in %s on line %d
