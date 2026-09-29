--TEST--
set_preempt_function(): pcntl_async_signals() and set_time_limit() keep working
--EXTENSIONS--
phasync
pcntl
--FILE--
<?php
use function phasync\ext\set_preempt_function;

function spin(float $s): void { $t = microtime(true); while (microtime(true) - $t < $s); }

$calls = [];
$where = 'other';
set_preempt_function(function () use (&$calls, &$where) {
    $calls[] = $where;
    if (Fiber::getCurrent()) Fiber::suspend();
}, 0.02);

echo "-- signals\n";
pcntl_async_signals(true);
$signals = 0;
pcntl_signal(SIGUSR1, function () use (&$signals, &$where) {
    $signals++;
    $where = 'signal';
    spin(0.2);             // fiber switching is blocked in a handler: skipped
    $where = 'other';
});
$f = new Fiber(function () {
    for ($i = 0; $i < 3; $i++) {
        exec('kill -USR1 ' . getmypid());
        spin(0.1);
    }
});
$f->start();
$resumes = 0;
while (!$f->isTerminated()) {
    $resumes++;
    $f->resume();
}
var_dump($signals, $resumes > 0, in_array('signal', $calls, true));

echo "-- time limit\n";
$calls = [];
register_shutdown_function(function () use (&$calls) {
    var_dump(count($calls) > 10);
});
set_time_limit(1);
while (true);
?>
--EXPECTF--
-- signals
int(3)
bool(true)
bool(false)
-- time limit

Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
bool(true)
