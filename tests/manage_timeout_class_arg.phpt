--TEST--
manage(): the 5th argument must name an existing Throwable class; it is scoped per (nested) manage()
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair');
?>
--FILE--
<?php
final class OuterTimeout extends Exception {}
final class InnerTimeout extends Exception {}
$noop = fn(...$a) => null;

foreach (['NoSuchClass', 'stdClass'] as $bad) {
    try {
        \phasync\ext\manage(fn() => null, $noop, $noop, $noop, $bad);
    } catch (ValueError $e) {
        echo $e->getMessage(), "\n";
    }
}

// Each scope uses its own class: inside the inner scope, OuterTimeout is NOT a
// timeout, so it propagates; InnerTimeout is.
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
$throw = fn(string $cls) => fn($res, $t) => throw new $cls();
\phasync\ext\manage(function () use ($a, $throw, $noop) {
    \phasync\ext\manage(function () use ($a) {
        (new Fiber(function () use ($a) {
            try { var_dump(fread($a, 10)); }
            catch (OuterTimeout $e) { echo "OuterTimeout propagated\n"; }
        }))->start();
    }, $throw(OuterTimeout::class), $noop, $noop, InnerTimeout::class);
    \phasync\ext\manage(function () use ($a) {
        (new Fiber(fn() => var_dump(fread($a, 10))))->start();   // timeout -> false
    }, $throw(InnerTimeout::class), $noop, $noop, InnerTimeout::class);
}, $noop, $noop, $noop, OuterTimeout::class);
fclose($a); fclose($b);
?>
--EXPECT--
phasync\ext\manage(): Argument #5 ($timeoutException) must be the name of an existing Throwable class
phasync\ext\manage(): Argument #5 ($timeoutException) must be the name of an existing Throwable class
OuterTimeout propagated
bool(false)
