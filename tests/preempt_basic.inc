<?php
use function phasync\ext\set_preempt_function;

function threads(): int { return count(scandir('/proc/self/task')) - 2; }

echo "-- none set\n";
$base = threads();
var_dump(set_preempt_function(null));
var_dump(threads() === $base);

echo "-- about every interval\n";
$n = 0;
$a = function () use (&$n) { $n++; };
var_dump(set_preempt_function($a, 0.1));
var_dump(threads() === $base + 1);
$t = microtime(true);
while (microtime(true) - $t < 1.0);
echo $n >= 5 && $n <= 10 ? "ok\n" : "calls: $n\n";

echo "-- loop without calls\n";
$n = 0;
set_preempt_function($a, 0.02);
$i = 0;
while ($n < 3) { $i++; }
var_dump($i > 0);

echo "-- returns previous, null removes\n";
$b = function () {};
var_dump(set_preempt_function($b) === $a);
var_dump(set_preempt_function(null) === $b);
var_dump(set_preempt_function(null));
var_dump(threads() === $base);
$n = 0;
$t = microtime(true);
while (microtime(true) - $t < 0.3);
var_dump($n);

echo "-- exception at the interrupted point\n";
function spin(): array
{
    $i = 0;
    try {
        while (true) { $i++; }
    } catch (RuntimeException $e) {
        return [$e->getMessage(), $e->getTrace()[1]['function'], $i > 0];
    }
}
function spinCalls(): int
{
    $s = '';
    while (true) { $s = str_repeat('x', 100) . $s; $s = substr($s, 0, 1000); }
}
$armed = true;
set_preempt_function(function () use (&$armed) {
    if ($armed) {
        $armed = false;
        throw new RuntimeException('preempted');
    }
}, 0.02);
var_dump(spin());
$armed = true;
try {
    spinCalls();
} catch (RuntimeException $e) {
    echo "caught in caller: ", $e->getMessage(), "\n";
}
set_preempt_function(null);

echo "-- suspend from the closure, resume into the loop\n";
$count = 0;
$outside = 0;
set_preempt_function(function () use (&$count, &$outside) {
    if (Fiber::getCurrent() === null) {
        $outside++;
        return;
    }
    if ($count === 5) return;   // the check after the loop runs unsuspended
    $count++;
    var_dump(Fiber::suspend('preempted'));
}, 0.02);
$f = new Fiber(function () use (&$count) {
    $sum = 0; $x = 7; $s = '';
    for ($i = 1; ; $i++) {
        $sum += $i;
        $x = ($x * 31 + $i) & 0xffffff;
        $s = substr($s . chr(65 + $i % 26), -26);
        if ($count >= 5) break;
    }
    $sum2 = 0; $y = 7; $s2 = '';
    for ($j = 1; $j <= $i; $j++) {
        $sum2 += $j;
        $y = ($y * 31 + $j) & 0xffffff;
        $s2 = substr($s2 . chr(65 + $j % 26), -26);
    }
    return $sum === $sum2 && $x === $y && $s === $s2;
});
$v = $f->start();
$resumes = 0;
while (!$f->isTerminated()) {
    var_dump($v);
    $resumes++;
    $t = microtime(true);
    while (microtime(true) - $t < 0.05);   // the loop's own code: called outside the fiber
    $v = $f->resume('resumed');
}
var_dump($f->getReturn(), $resumes, $outside > 0);
set_preempt_function(null);

echo "-- bad interval\n";
foreach ([0.0, -1.0, INF, NAN] as $bad) {
    try { set_preempt_function($a, $bad); } catch (ValueError $e) { echo $e->getMessage(), "\n"; }
}
var_dump(set_preempt_function(null));
