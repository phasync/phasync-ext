--TEST--
Hooked sleep()/usleep()/time_nanosleep()/time_sleep_until() call the sleep handler
--EXTENSIONS--
phasync
--FILE--
<?php
$calls = [];
\phasync\enable_hooks();
\phasync\register_sleep_handler(function($usec) use (&$calls) { $calls[] = $usec; });

var_dump(sleep(2));                       // -> 2000000 us, returns 0
usleep(500);                              // -> 500 us
var_dump(time_nanosleep(1, 500000000));   // 1s + 500ms -> 1500000 us, returns true
$rv = time_sleep_until(microtime(true) + 0.25); // ~250000 us, returns true
var_dump($rv);
var_dump($calls[0], $calls[1], $calls[2]);
// 4th call ~250000us; allow scheduling slack
var_dump($calls[3] >= 240000 && $calls[3] <= 260000);

// no handler -> real functions run
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
bool(true)
