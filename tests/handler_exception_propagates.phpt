--TEST--
A non-timeout exception from park() propagates out of the hooked op unchanged, and leaves the stream consistent
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
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);

// park() throws on the first wait (a cancellation, or the loop refusing a second
// waiter): the exception surfaces from fread() itself, not swallowed and not
// turned into a timeout.
$loop = new Loop;
$loop->onPark = fn() => throw new RuntimeException('cancelled');
$loop->runAll(function () use ($a) {
    try {
        fread($a, 100);                                  // empty -> would block -> throws
        echo "no exception\n";
    } catch (RuntimeException $e) {
        echo "caught: ", $e->getMessage(), "\n";
    }
    var_dump(stream_get_meta_data($a)['timed_out']);     // bool(false): not a timeout
});

// The stream is left consistent: nothing was consumed, and it works normally
// both outside a scope (native blocking read) and inside one (cooperative read).
fwrite($b, 'one');
var_dump(fread($a, 100));                                // string(3) "one"

(new Loop)->runAll(
    fn() => print('two: ' . fread($a, 100) . "\n"),
    function () use ($b) { usleep(50000); fwrite($b, 'two'); },
);
fclose($a); fclose($b);
?>
--EXPECT--
caught: cancelled
bool(false)
string(3) "one"
two: two
