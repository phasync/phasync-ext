--TEST--
set_preempt_function(): loops are interrupted between iterations, code without loops is not (tracing JIT)
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!extension_loaded('Zend OPcache')) die('skip needs opcache'); ?>
--INI--
opcache.enable=1
opcache.enable_cli=1
opcache.jit=tracing
opcache.jit_buffer_size=64M
--FILE--
<?php
// The loops call no user function: under the tracing JIT, a loop whose trace
// links into the trace of a function it calls may have no interrupt check at its
// head, and then is not interrupted (see README).
use function phasync\ext\set_preempt_function;

var_dump(opcache_get_status()['jit']['on']);
$phase = 0;
$calls = 0;
$mid = 0;
set_preempt_function(function () use (&$phase, &$calls, &$mid) {
    $calls++;
    if ($phase !== 0) $mid++;
}, 0.005);

function loop(string $name, Closure $loop): void
{
    global $calls, $mid;
    $calls = $mid = 0;
    $loop(microtime(true) + 0.1);
    echo "$name: ", $calls >= 3 ? "interrupted" : "calls: $calls", ", mid-iteration: $mid\n";
}

$arr = range(1, 1000);
loop('while', function (float $end) {
    global $phase;
    $x = 0;
    while (microtime(true) < $end) {
        $phase = 1; $x = ($x * 7 + 3) % 1001; if ($x > 500) { $x -= 3; } $phase = 0;
    }
});
loop('for', function (float $end) {
    global $phase;
    $x = 0;
    for ($i = 0; microtime(true) < $end; $i++) {
        $phase = 1; $x = ($x + $i) % 1001; $phase = 0;
    }
});
loop('do-while', function (float $end) {
    global $phase;
    $x = 0;
    do {
        $phase = 1; $x = ($x * 3 + 1) % 1001; $phase = 0;
    } while (microtime(true) < $end);
});
loop('foreach with continue, nested', function (float $end) use ($arr) {
    global $phase;
    $x = 0;
    while (microtime(true) < $end) {
        foreach ($arr as $v) {
            $phase = 1;
            if ($v % 2) { $phase = 0; continue; }
            $x = ($x + $v) % 1001; $phase = 0;
        }
    }
});

function fib(int $n): int { return $n < 2 ? $n : fib($n - 1) + fib($n - 2); }
$n = 15;
do {
    $calls = 0;
    $t = microtime(true);
    fib($n++);
    $got = $calls;
} while (microtime(true) - $t < 0.1);
echo "recursion: calls: $got\n";
set_preempt_function(null);
?>
--EXPECT--
bool(true)
while: interrupted, mid-iteration: 0
for: interrupted, mid-iteration: 0
do-while: interrupted, mid-iteration: 0
foreach with continue, nested: interrupted, mid-iteration: 0
recursion: calls: 0
