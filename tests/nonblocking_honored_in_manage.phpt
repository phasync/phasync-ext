--TEST--
Inside manage(), an explicitly non-blocking stream is honored (no suspension)
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!function_exists('stream_socket_server')) die('skip requires sockets'); ?>
--FILE--
<?php
$srv  = stream_socket_server('tcp://127.0.0.1:0', $e, $es);
$name = stream_socket_get_name($srv, false);
$port = substr($name, strrpos($name, ':') + 1);
$cli  = stream_socket_client("tcp://127.0.0.1:$port", $e, $es, 2);
$conn = stream_socket_accept($srv, 1);

$called = false;
$mark = function ($s) use (&$called) { $called = true; };

$r = \phasync\ext\manage(function () use ($cli) {
    stream_set_blocking($cli, false);      // caller opts out of blocking
    return fread($cli, 100);               // no data -> must NOT suspend
}, $mark, $mark, fn($us) => null);

var_dump($r === '' || $r === false);       // bool(true): got would-block, natively
var_dump($called);                         // bool(false): the wait handler never ran

fclose($cli); fclose($conn); fclose($srv);
?>
--EXPECT--
bool(true)
bool(false)
