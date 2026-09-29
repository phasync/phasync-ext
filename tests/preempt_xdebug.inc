<?php
// GH #31: an observed checkpoint() (Xdebug's fcall observer, coverage or
// develop mode) used to corrupt memory near the call because checkpoint()
// was hidden from CG(function_table) before zend_observer_post_startup()
// could give it the extra call-frame temp an observed function needs
// (zend_observer.c's prev_observed_frame()). It crashed inside a Fiber, on
// whatever internal call happened to land on the corrupted memory next --
// not necessarily the checkpoint() call itself -- so this exercises loops
// (which carry the compiled-in checkpoints), internal calls after them, and
// a Fiber, the way a phasync coroutine would.
use function phasync\ext\set_preempt_function;

$calls = 0;
set_preempt_function(function () use (&$calls) { $calls++; }, 0.005);

function work(float $end): array
{
    $out = [];
    $i = 0;
    while (microtime(true) < $end) {
        $i++;
        $out[] = strtoupper(str_repeat('x', $i % 7 + 1));
        if (count($out) > 50) {
            sort($out);
            $out = array_slice($out, 0, 10);
        }
    }
    return $out;
}

$fiber = new Fiber(function () {
    return work(microtime(true) + 0.2);
});
$fiber->start();
$result = $fiber->getReturn();

set_preempt_function(null);

echo $calls >= 1 ? "preempted\n" : "not preempted (calls: $calls)\n";
echo is_array($result) ? "result ok\n" : "result corrupted\n";
echo "done\n";
