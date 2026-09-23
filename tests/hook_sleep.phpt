--TEST--
Hooked sleep()/usleep()/time_nanosleep()/time_sleep_until(): intercept inside a fiber only
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
$calls = [];
\phasync\enable_hooks();
\phasync\register_sleep_handler(function($usec) use (&$calls) { $calls[] = $usec; });

/* Inside a fiber: the sleep functions delegate to the handler (which here just
 * records the requested duration and returns without actually waiting). */
$f = new Fiber(function () {
    var_dump(sleep(2));                              // -> 2000000 us, returns 0
    usleep(500);                                     // -> 500 us
    var_dump(time_nanosleep(1, 500000000));          // 1.5s -> 1500000 us, true
    var_dump(time_sleep_until(microtime(true) + 0.25)); // ~250000 us, true
});
$f->start();

var_dump($calls[0], $calls[1], $calls[2]);
var_dump($calls[3] >= 240000 && $calls[3] <= 260000);

/* Outside a fiber, hooks must NOT intercept: the scheduler itself idle-waits by
 * calling these same functions from the main context, and delegating that to a
 * handler that can only suspend inside a fiber would spin the loop. So here the
 * handler is not called and a real sleep happens. */
$calls2 = [];
\phasync\register_sleep_handler(function($usec) use (&$calls2) { $calls2[] = $usec; });
$t = microtime(true);
usleep(1000);
var_dump($calls2);                                   // empty: handler NOT called
var_dump(microtime(true) - $t >= 0.0005);            // real sleep happened

/* No handler at all -> the real functions run regardless of context. */
\phasync\register_sleep_handler(null);
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
array(0) {
}
bool(true)
bool(true)
