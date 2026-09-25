--TEST--
Wrapped streams behave natively (blocking/non-blocking) outside any manage() scope
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!function_exists('stream_socket_server')) die('skip requires sockets'); ?>
--FILE--
<?php
// A loopback socket goes through the tcp factory, so it is wrapped. With no
// manage() scope active the wrapper must be indistinguishable from a raw socket.
$srv  = stream_socket_server('tcp://127.0.0.1:0', $e, $es);
$name = stream_socket_get_name($srv, false);
$port = substr($name, strrpos($name, ':') + 1);
$cli  = stream_socket_client("tcp://127.0.0.1:$port", $e, $es, 2);
$conn = stream_socket_accept($srv, 1);

// 1. Blocking read (the default): data written is read back.
fwrite($conn, 'hello');
usleep(20000);
var_dump(fread($cli, 100));                 // string(5) "hello"

// 2. Non-blocking read with no data pending must return "" immediately,
//    never block and never suspend (there is no fiber here anyway).
stream_set_blocking($cli, false);
$r = fread($cli, 100);
var_dump($r === '' || $r === false);        // bool(true)

// 3. Data arrives while non-blocking: it is delivered.
fwrite($conn, 'abc');
usleep(20000);
var_dump(fread($cli, 100));                 // string(3) "abc"

// 4. Back to blocking, and after a manage() scope has come and gone the stream
//    is still perfectly native.
stream_set_blocking($cli, true);
\phasync\ext\manage(fn() => null, fn($x) => null, fn($x) => null, fn($x) => null);
fwrite($conn, 'world');
usleep(20000);
var_dump(fread($cli, 100));                 // string(5) "world"

fclose($cli); fclose($conn); fclose($srv);
?>
--EXPECT--
string(5) "hello"
bool(true)
string(3) "abc"
string(5) "world"
