--TEST--
fread() after fgets() left bytes in PHP's buffer: returns them like native (8.3+: no wait; 8.2: waits the timeout)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair');
?>
--FILE--
<?php
// fgets() reads a chunk, returns one line and leaves the rest buffered. The next
// fread() gets those bytes first and then asks the socket for more. Since PHP 8.3
// the native socket read does not wait when the call already has data
// (stream->has_buffered_data), so fread() returns at once; 8.2 waited for the
// timeout first. The wrapper must match either way — on 8.3+ waiting here would
// hang a request/response protocol whose peer is waiting for our reply.
final class TestTimeout extends Exception {}

[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
stream_set_timeout($a, 0, 300000);
fwrite($b, "line1\nextra");

$waited = false;
(new Fiber(fn() => \phasync\ext\manage(function () use ($a, &$waited) {
    var_dump(fgets($a));                                    // "line1\n"
    var_dump(fread($a, 100));                               // "extra"
    var_dump($waited === (PHP_VERSION_ID < 80300));         // bool(true)
    var_dump(stream_get_meta_data($a)['timed_out'] === (PHP_VERSION_ID < 80300)); // bool(true)
}, function ($s, ?float $t) use (&$waited) { $waited = true; throw new TestTimeout(); },
   fn($s, $t) => null, fn($us) => null, TestTimeout::class)))->start();
fclose($a); fclose($b);
?>
--EXPECT--
string(6) "line1
"
string(5) "extra"
bool(true)
bool(true)
