--TEST--
set_preempt_function(): a fork()ed child keeps the closure and has a timer of its own
--EXTENSIONS--
phasync
pcntl
--SKIPIF--
<?php if (!is_dir('/proc/self/task')) die('skip needs /proc'); ?>
--FILE--
<?php
use function phasync\ext\set_preempt_function;

function spin(float $s): void { $t = microtime(true); while (microtime(true) - $t < $s); }
function threads(): int { clearstatcache(true); return count(scandir('/proc/self/task')) - 2; }   // after fork(), /proc/self must not come from the realpath cache (php/php-src#23986)

$base = threads();
$n = 0;
set_preempt_function(function () use (&$n) { $n++; }, 0.05);

$pid = pcntl_fork();
if ($pid === 0) {
    $n = 0;
    spin(0.5);
    echo "child: ", $n >= 4 && $n <= 10 ? "ok" : "calls: $n", ", threads: ", threads() - $base, "\n";
    var_dump(set_preempt_function(null) instanceof Closure, threads() === $base);
    exit(0);
}
pcntl_waitpid($pid, $status);
var_dump(pcntl_wexitstatus($status));
$n = 0;
spin(0.5);
echo "parent: ", $n >= 4 && $n <= 10 ? "ok" : "calls: $n", ", threads: ", threads() - $base, "\n";

echo "-- none set: the child has no timer\n";
set_preempt_function(null);
$pid = pcntl_fork();
if ($pid === 0) {
    echo "child threads: ", threads() - $base, "\n";
    exit(0);
}
pcntl_waitpid($pid, $status);
?>
--EXPECT--
child: ok, threads: 1
bool(true)
bool(true)
int(0)
parent: ok, threads: 1
-- none set: the child has no timer
child threads: 0
