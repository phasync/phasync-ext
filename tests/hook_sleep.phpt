--TEST--
Hooked sleep()/usleep() invoke the sleep handler with microseconds
--EXTENSIONS--
phasync
--FILE--
<?php
$calls = [];
\phasync\enable_hooks();
\phasync\register_sleep_handler(function($usec) use (&$calls) { $calls[] = $usec; });
$rv = sleep(2);
usleep(500);
var_dump($rv);
var_dump($calls);
// with no handler, real sleep runs (clear handler)
\phasync\register_sleep_handler(null);
$t = microtime(true);
usleep(1000);
var_dump(microtime(true) - $t >= 0.0005);
?>
--EXPECT--
int(0)
array(2) {
  [0]=>
  int(2000000)
  [1]=>
  int(500)
}
bool(true)
