--TEST--
set_preempt_function(): skipped in the cycle collector's destructors and in a fiber being destroyed
--EXTENSIONS--
phasync
--FILE--
<?php
use function phasync\ext\set_preempt_function;

function spin(float $s): void { $t = microtime(true); while (microtime(true) - $t < $s); }

$calls = [];
$where = 'other';
// Suspending where it is skipped would throw FiberError (a fiber being destroyed)
// or leave the destructor suspended (the collector's destructor fiber, 8.4+).
set_preempt_function(function () use (&$calls, &$where) {
    $calls[] = $where;
    if (Fiber::getCurrent()) Fiber::suspend();
}, 0.02);

class Cycle
{
    public $self;
    function __destruct()
    {
        global $where;
        $where = 'gc';
        spin(0.2);
        $where = 'other';
        echo "destructor done\n";
    }
}

echo "-- the cycle collector's destructors, in a fiber\n";
$f = new Fiber(function () {
    $c = new Cycle;
    $c->self = $c;
    unset($c);
    var_dump(gc_collect_cycles() > 0);
});
$f->start();
while (!$f->isTerminated()) $f->resume();

echo "-- and outside any fiber\n";
$c = new Cycle;
$c->self = $c;
unset($c);
var_dump(gc_collect_cycles() > 0);
var_dump(in_array('gc', $calls, true));

echo "-- a fiber being destroyed\n";
$g = new Fiber(function () use (&$where) {
    try {
        Fiber::suspend();
    } finally {
        $where = 'destroyed';
        spin(0.2);
        $where = 'other';
        echo "finally done\n";
    }
});
$g->start();
unset($g);
var_dump(in_array('destroyed', $calls, true));

echo "-- called again afterwards\n";
$calls = [];
spin(0.1);
var_dump(count($calls) > 0);
?>
--EXPECT--
-- the cycle collector's destructors, in a fiber
destructor done
bool(true)
-- and outside any fiber
destructor done
bool(true)
bool(false)
-- a fiber being destroyed
finally done
bool(false)
-- called again afterwards
bool(true)
