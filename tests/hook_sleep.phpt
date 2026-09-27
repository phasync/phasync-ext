--TEST--
manage(): the sleep functions go to the loop's sleep only in a coroutine; a real sleep otherwise
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
$loop->instantSleep = true;                              // record, don't wait
$loop->manage(function () use ($loop) {
    // In a coroutine: the sleep functions go to the loop's sleep.
    $loop->go(function () {
        var_dump(sleep(2));                              // -> 2000000 us, returns 0
        usleep(500);                                     // -> 500 us
        var_dump(time_nanosleep(1, 500000000));          // 1.5s -> 1500000 us, true
        var_dump(time_sleep_until(microtime(true) + 0.25)); // ~250000 us, true
    });
    $loop->run();
    $calls = $loop->sleepCalls;
    var_dump($calls[0], $calls[1], $calls[2]);
    var_dump($calls[3] >= 240000 && $calls[3] <= 260000);

    // Inside manage() but NOT in a coroutine (the loop's own idle waits look like
    // this): a real sleep, not the loop's.
    $t = microtime(true);
    usleep(1000);
    var_dump(count($loop->sleepCalls) === 4);
    var_dump(microtime(true) - $t >= 0.0005);
});

// Outside any manage() scope: a real sleep.
$t = microtime(true);
usleep(1000);
var_dump(microtime(true) - $t >= 0.0005);
?>
--EXPECT--
int(0)
bool(true)
bool(true)
int(2000000)
int(500)
int(1500000)
bool(true)
bool(true)
bool(true)
bool(true)
