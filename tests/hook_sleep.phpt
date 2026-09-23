--TEST--
manage(): sleep handler fires only inside a fiber; real sleep otherwise
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
$calls = [];
$sleepH = function ($usec) use (&$calls) { $calls[] = $usec; };   // record, don't wait
$noop   = fn($fd) => null;

\phasync\ext\manage(function () use (&$calls) {
    // Inside a fiber: the sleep functions delegate to the handler.
    $f = new Fiber(function () {
        var_dump(sleep(2));                              // -> 2000000 us, returns 0
        usleep(500);                                     // -> 500 us
        var_dump(time_nanosleep(1, 500000000));          // 1.5s -> 1500000 us, true
        var_dump(time_sleep_until(microtime(true) + 0.25)); // ~250000 us, true
    });
    $f->start();
    var_dump($calls[0], $calls[1], $calls[2]);
    var_dump($calls[3] >= 240000 && $calls[3] <= 260000);

    // Inside manage() but NOT in a fiber: the scheduler's own idle-waits look
    // like this, and must fall through to a real sleep (the handler cannot
    // suspend outside a fiber), so the handler is NOT called here.
    $before = count($calls);
    $t = microtime(true);
    usleep(1000);
    var_dump(count($calls) === $before);                 // handler not called
    var_dump(microtime(true) - $t >= 0.0005);            // real sleep happened
}, $noop, $noop, $sleepH);

// Outside any manage() scope: no handlers at all -> real sleep.
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
