--TEST--
set_preempt_function(): not re-entrant; a call suspended in one fiber doesn't hold back others
--EXTENSIONS--
phasync
--FILE--
<?php
use function phasync\ext\set_preempt_function;

function spin(float $s): void { $t = microtime(true); while (microtime(true) - $t < $s); }

echo "-- a slow closure\n";
$depth = 0;
$max = 0;
$calls = 0;
set_preempt_function(function () use (&$depth, &$max, &$calls) {
    $calls++;
    $max = max($max, ++$depth);
    spin(0.3);             // many intervals pass in here
    $depth--;
}, 0.02);
spin(0.7);
set_preempt_function(null);
var_dump($max, $calls >= 1 && $calls <= 3);

echo "-- a call suspended in a fiber\n";
$log = [];
$stop = false;
$names = new WeakMap;
set_preempt_function(function () use (&$log, $names) {
    if ($f = Fiber::getCurrent()) {
        $log[] = $names[$f];
        Fiber::suspend();
    }
}, 0.02);
$a = new Fiber(function () use (&$stop) { while (!$stop); });
$names[$a] = 'a';
$b = new Fiber(function () use (&$stop) { while (!$stop); });
$names[$b] = 'b';
$a->start();               // preempted: a's call is suspended
$b->start();               // b is preempted meanwhile
$a->resume();              // a's call returns, a is preempted again
$stop = true;
set_preempt_function(null);
$a->resume();
$b->resume();
var_dump($log, $a->isTerminated(), $b->isTerminated());
?>
--EXPECT--
-- a slow closure
int(1)
bool(true)
-- a call suspended in a fiber
array(3) {
  [0]=>
  string(1) "a"
  [1]=>
  string(1) "b"
  [2]=>
  string(1) "a"
}
bool(true)
bool(true)
