<?php
use function phasync\ext\set_preempt_function;

// $phase is 0 exactly between iterations: the closure must never see it set.
// On PHP < 8.4 without JIT, a do-while or a loop left by break is interrupted
// less often when its time goes into calls (see README): those call nothing.
$phase = 0;
$calls = 0;
$mid = 0;
set_preempt_function(function () use (&$phase, &$calls, &$mid) {
    $calls++;
    if ($phase !== 0) $mid++;
}, 0.005);

function f(int $x): int { return $x + 1; }
function g(int $x): int { return $x > 3 ? f($x) : f($x + 1); }

function loop(string $name, Closure $loop): void
{
    global $calls, $mid;
    $calls = $mid = 0;
    $loop(microtime(true) + 0.1);
    echo "$name: ", $calls >= 3 ? "interrupted" : "calls: $calls", ", mid-iteration: $mid\n";
}

// Runs $code on ever more work until one run takes several intervals.
function none(string $name, Closure $code): void
{
    global $calls;
    $size = 1000;
    do {
        $calls = 0;
        $t = microtime(true);
        $code($size);
        $got = $calls;
        $size *= 2;
    } while (microtime(true) - $t < 0.05);
    echo "$name: calls: $got\n";
}

$arr = range(1, 1000);

echo "-- loops are interrupted between iterations\n";
loop('while', function (float $end) {
    global $phase;
    while (microtime(true) < $end) {
        $phase = 1; $x = g(1); if ($x > 0) { $x = f($x); } $phase = 0;
    }
});
loop('for', function (float $end) {
    global $phase;
    for ($i = 0; microtime(true) < $end; $i++) {
        $phase = 1; $x = g($i); $phase = 0;
    }
});
loop('do-while', function (float $end) {
    global $phase;
    $x = 0;
    do {
        $phase = 1; $x = ($x * 3 + 1) % 1001; $phase = 0;
    } while (microtime(true) < $end);
});
loop('foreach in do-while', function (float $end) use ($arr) {
    global $phase;
    do {
        foreach ($arr as $v) {
            $phase = 1; $x = g($v); $phase = 0;
        }
    } while (microtime(true) < $end);
});
loop('while(true) with break', function (float $end) {
    global $phase;
    $x = 0;
    while (true) {
        $phase = 1; $x = ($x * 7 + 3) % 1001; $phase = 0;
        if (microtime(true) >= $end) break;
    }
});
loop('while with continue', function (float $end) {
    global $phase;
    while (microtime(true) < $end) {
        $phase = 1;
        if (g(1) > 0) { $phase = 0; continue; }
        $phase = 2;
    }
});
loop('foreach with continue', function (float $end) use ($arr) {
    global $phase;
    while (microtime(true) < $end) {
        foreach ($arr as $v) {
            $phase = 1;
            if ($v % 2) { $phase = 0; continue; }
            $x = f($v); $phase = 0;
        }
    }
});
loop('nested for', function (float $end) {
    global $phase;
    for ($i = 0; microtime(true) < $end; $i++) {
        for ($j = 0; $j < 100; $j++) {
            $phase = 1; $x = g($j); $phase = 0;
        }
    }
});

echo "-- code without a loop is not\n";
function fib(int $n): int { return $n < 2 ? $n : fib($n - 1) + fib($n - 2); }
none('recursion', function (int $size) { fib((int) log($size, 1.6)); });
none('array_map', function (int $size) { array_map(fn($x) => g($x), range(1, $size)); });
none('usort', function (int $size) { $a = range($size, 1); usort($a, fn($x, $y) => g($x) <=> g($y)); });
set_preempt_function(null);
