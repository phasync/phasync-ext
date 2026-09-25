--TEST--
phasync\ext\is_managed(): true for wrapped connection sockets, false for listeners/non-fd streams
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!function_exists('stream_socket_server')) die('skip requires sockets'); ?>
--FILE--
<?php
use function phasync\ext\is_managed;

// Not even a resource.
var_dump(is_managed(42));               // bool(false)
var_dump(is_managed('nope'));           // bool(false)

// php://memory is not descriptor-backed, so it is never wrapped.
$mem = fopen('php://memory', 'r+');
var_dump(is_managed($mem));             // bool(false)

// A listening server socket: accept() is not intercepted -> not managed.
$srv  = stream_socket_server('tcp://127.0.0.1:0', $e, $es);
var_dump(is_managed($srv));             // bool(false)

$name = stream_socket_get_name($srv, false);
$port = substr($name, strrpos($name, ':') + 1);

// A connected client socket is wrapped and drivable -> managed.
$cli = stream_socket_client("tcp://127.0.0.1:$port", $e, $es, 2);
var_dump(is_managed($cli));             // bool(true)

// The accepted server-side connection inherits wrapped ops -> managed.
$conn = stream_socket_accept($srv, 1);
var_dump(is_managed($conn));            // bool(true)

fclose($cli); fclose($conn); fclose($srv); fclose($mem);
?>
--EXPECT--
bool(false)
bool(false)
bool(false)
bool(false)
bool(true)
bool(true)
