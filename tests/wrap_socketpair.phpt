--TEST--
stream_socket_pair() streams are wrapped and cooperate (fread suspends, resumes with data)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
// A blocking fread on the empty pair parks the coroutine until the other end is written.
$loop->runAll(
    function () use ($a) { echo "read: " . fread($a, 100) . "\n"; },
    function () use ($b) { usleep(50000); fwrite($b, 'ping'); },
);
var_dump($loop->parks > 0);
fclose($a); fclose($b);
?>
--EXPECT--
read: ping
bool(true)
