--TEST--
Inside manage(), an explicitly non-blocking stream is honored (no suspension)
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!function_exists('stream_socket_server')) die('skip requires sockets'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
$srv  = stream_socket_server('tcp://127.0.0.1:0', $e, $es);
$name = stream_socket_get_name($srv, false);
$cli  = stream_socket_client("tcp://$name", $e, $es, 2);
$conn = stream_socket_accept($srv, 1);

$r = null;
$loop->runAll(function () use ($cli, &$r) {
    stream_set_blocking($cli, false);      // caller opts out of blocking
    $r = fread($cli, 100);                 // no data -> must NOT park
});
var_dump($r === '' || $r === false);       // bool(true): got would-block, natively
var_dump($loop->parks);                    // int(0): never parked

fclose($cli); fclose($conn); fclose($srv);
?>
--EXPECT--
bool(true)
int(0)
