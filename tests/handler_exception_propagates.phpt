--TEST--
A non-timeout handler exception propagates out of the hooked op unchanged, and leaves the stream consistent
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair');
?>
--FILE--
<?php
final class TestTimeout extends Exception {}

[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);

// A handler that throws on the very first wait (e.g. a coroutine cancellation,
// or phasync refusing a second waiter): the exception surfaces from fread()
// itself — it is not swallowed and not turned into a timeout.
$boom = function ($res, $t) { throw new RuntimeException('cancelled'); };
\phasync\ext\manage(function () use ($a) {
    (new Fiber(function () use ($a) {
        try {
            fread($a, 100);                                  // empty -> would block -> throws
            echo "no exception\n";
        } catch (RuntimeException $e) {
            echo "caught: ", $e->getMessage(), "\n";
        }
        var_dump(stream_get_meta_data($a)['timed_out']);     // bool(false): not a timeout
    }))->start();
}, $boom, $boom, fn($us) => null, TestTimeout::class);

// The stream is left consistent: nothing was consumed, and it works normally
// both outside a scope (native blocking read) and inside one (cooperative read).
fwrite($b, 'one');
var_dump(fread($a, 100));                                    // string(3) "one"

\phasync\ext\manage(function () use ($a, $b) {
    $f = new Fiber(fn() => print('two: ' . fread($a, 100) . "\n"));
    $res = $f->start();                                      // suspends waiting for data
    fwrite($b, 'two');
    $r = [$res]; $w = $e = null;
    \phasync\ext\stream_select($r, $w, $e, 2);
    $f->resume();
}, fn($res) => Fiber::suspend($res), fn($res) => Fiber::suspend($res), fn($us) => null, TestTimeout::class);

fclose($a); fclose($b);
?>
--EXPECT--
caught: cancelled
bool(false)
string(3) "one"
two: two
