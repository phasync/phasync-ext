--TEST--
phasync\ext\is_auto_managed(): true only for a wrapped, blocking, non-listener stream inside a scope
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!function_exists('stream_socket_server')) die('skip requires sockets'); ?>
--FILE--
<?php
use function phasync\ext\is_auto_managed;

// Not a resource, and non-descriptor streams: never auto-managed.
var_dump(is_auto_managed(42));               // bool(false)
$mem = fopen('php://memory', 'r+');
var_dump(is_auto_managed($mem));             // bool(false)

$srv  = stream_socket_server('tcp://127.0.0.1:0', $e, $es);
$name = stream_socket_get_name($srv, false);
$port = substr($name, strrpos($name, ':') + 1);
$cli  = stream_socket_client("tcp://127.0.0.1:$port", $e, $es, 2);
$conn = stream_socket_accept($srv, 1);

// Outside a scope nothing auto-manages, even a wrapped socket.
var_dump(is_auto_managed($cli));             // bool(false)

\phasync\ext\manage(function () use ($cli, $srv) {
    var_dump(is_auto_managed($cli));         // bool(true): wrapped, blocking, in scope
    var_dump(is_auto_managed($srv));         // bool(false): listener
    stream_set_blocking($cli, false);
    var_dump(is_auto_managed($cli));         // bool(false): caller opted non-blocking
    stream_set_blocking($cli, true);
    var_dump(is_auto_managed($cli));         // bool(true): blocking again
}, fn($x) => null, fn($x) => null, fn($us) => null);

// Scope gone -> false again.
var_dump(is_auto_managed($cli));             // bool(false)

fclose($cli); fclose($conn); fclose($srv); fclose($mem);
?>
--EXPECT--
bool(false)
bool(false)
bool(false)
bool(true)
bool(false)
bool(false)
bool(true)
bool(false)
