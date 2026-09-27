--TEST--
manage(): the 6th argument must name an existing Throwable class; it is scoped per (nested) manage(), and only ends a wait whose native timeout ran out
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
final class OuterTimeout extends Exception {}
final class InnerTimeout extends Exception {}
$noop = fn(...$a) => null;

foreach (['NoSuchClass', 'stdClass'] as $bad) {
    try {
        \phasync\ext\manage(fn() => null, $noop, $noop, $noop, $noop, $bad);
    } catch (ValueError $e) {
        echo $e->getMessage(), "\n";
    }
}

// Each scope uses its own class: inside the inner scope, OuterTimeout is not a
// timeout, so it propagates; InnerTimeout is: the read times out natively (false).
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
stream_set_timeout($a, 0, 1);                           // the native timeout: 1 µs
$outer = new Loop;
$outer->manage(function () use ($a) {
    $l = new Loop; $l->parkThrows = OuterTimeout::class;
    $l->manage(function () use ($a, $l) {
        $l->go(function () use ($a) {
            try { var_dump(fread($a, 10)); }
            catch (OuterTimeout $e) { echo "OuterTimeout propagated\n"; }
        });
        $l->run();
    }, InnerTimeout::class);
    $l = new Loop; $l->parkThrows = InnerTimeout::class;
    $l->manage(function () use ($a, $l) {
        $l->go(fn() => var_dump(fread($a, 10)));        // timeout -> false
        $l->run();
    }, InnerTimeout::class);
}, OuterTimeout::class);

// A timeout exception before the native timeout ran out is not the native one
// (a cancellation someone sent as a timeout): it propagates.
stream_set_timeout($a, 60);
$l = new Loop; $l->parkThrows = LoopTimeout::class;
$l->runAll(function () use ($a) {
    try { var_dump(fread($a, 10)); }
    catch (LoopTimeout $e) { echo "early LoopTimeout propagated\n"; }
});
fclose($a); fclose($b);
?>
--EXPECT--
phasync\ext\manage(): Argument #6 ($timeoutException) must be the name of an existing Throwable class
phasync\ext\manage(): Argument #6 ($timeoutException) must be the name of an existing Throwable class
OuterTimeout propagated
bool(false)
early LoopTimeout propagated
