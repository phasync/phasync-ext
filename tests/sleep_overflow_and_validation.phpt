--TEST--
Sleep hooks: absurdly long sleeps clamp instead of overflowing; time_nanosleep() validates nanoseconds like native
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
final class TestTimeout extends Exception {}
$got = [];
$record = function (int $us) use (&$got) { $got[] = $us; };
(new Fiber(fn() => \phasync\ext\manage(function () {
    sleep(2);
    sleep(PHP_INT_MAX);                                 // seconds -> µs used to overflow
    time_nanosleep(PHP_INT_MAX, 999999999);
    time_sleep_until(1e300);                            // double -> long was out of range
    try {
        time_nanosleep(0, 1000000000);
    } catch (ValueError $e) {
        echo get_class($e), ': ', $e->getMessage(), "\n";
    }
}, fn($s, $t) => null, fn($s, $t) => null,
   $record, TestTimeout::class)))->start();

var_dump($got[0]);                                      // int(2000000)
var_dump($got[1] === PHP_INT_MAX, $got[2] === PHP_INT_MAX, $got[3] === PHP_INT_MAX);
?>
--EXPECT--
ValueError: Nanoseconds was not in the range 0 to 999 999 999 or seconds was negative
int(2000000)
bool(true)
bool(true)
bool(true)
