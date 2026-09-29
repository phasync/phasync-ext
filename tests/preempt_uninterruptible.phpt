--TEST--
set_preempt_function(): not called while an #[Uninterruptible] function, or anything it calls, runs
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair'); ?>
--FILE--
<?php
use function phasync\ext\set_preempt_function;
use phasync\Uninterruptible;

function spin(float $s): void { $t = microtime(true); while (microtime(true) - $t < $s); }

#[Uninterruptible]
function loopInside(): void { $t = microtime(true); while (microtime(true) - $t < 0.1); }

#[Uninterruptible]
function callsALoop(): void { spin(0.1); }

class K
{
    #[Uninterruptible]
    public function method(): void { spin(0.1); }
}

$calls = 0;
set_preempt_function(function () use (&$calls) { $calls++; }, 0.005);

function measure(string $name, callable $code): void
{
    global $calls;
    $calls = 0;
    $code();
    echo "$name: ", $calls >= 3 ? "interrupted" : "calls: $calls", "\n";
}

measure('a loop in it', 'loopInside');
measure('a loop in a function it calls', 'callsALoop');
measure('a closure', #[Uninterruptible] function () { spin(0.1); });
measure('a method', [new K, 'method']);
measure('the same loop outside', fn() => spin(0.1));

var_dump((new ReflectionFunction('loopInside'))->getAttributes()[0]->newInstance() instanceof Uninterruptible);

echo "-- suspended on I/O, others run and are preempted meanwhile\n";
require __DIR__ . '/loop.inc';
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
$preempted = 0;
set_preempt_function(function () use (&$preempted) {
    if (Fiber::getCurrent()) {
        $preempted++;
        usleep(1);          // yields to the loop
    }
}, 0.005);

#[Uninterruptible]
function readIt($stream): string { return fread($stream, 10); }

$done = false;
$loop = new Loop;
$loop->runAll(
    function () use ($a, &$done, &$preempted) {
        $got = readIt($a);
        echo "read $got, the spinner was preempted meanwhile: ", $preempted > 0 ? 'yes' : 'no', "\n";
        $done = true;
    },
    function () use (&$done) {
        while (!$done);
    },
    function () use ($b) {
        usleep(50000);
        fwrite($b, 'data');
    },
);
set_preempt_function(null);
?>
--EXPECT--
a loop in it: calls: 0
a loop in a function it calls: calls: 0
a closure: calls: 0
a method: calls: 0
the same loop outside: interrupted
bool(true)
-- suspended on I/O, others run and are preempted meanwhile
read data, the spinner was preempted meanwhile: yes
