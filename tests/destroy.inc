<?php
// Destroy a coroutine while it is suspended inside an operation, as PHP does to a
// fiber nobody references any more (an application dropping a coroutine, or
// phasync tearing down a failed run): by refcount (its last reference unset) or
// by the cycle collector (the fiber in a reference cycle). PHP resumes it once to
// unwind it: its finally blocks run, and it can no longer suspend.
require_once __DIR__ . '/loop.inc';

/**
 * Under $loop's manage() scope, start $op in a coroutine and, once it is
 * suspended, destroy it by $how ('refcount' or 'gc'); then run the loop, with
 * the coroutines $after. Coroutines in $before start (up to their first
 * suspension) before $op, those in $alongside after it, before it is destroyed.
 * Prints what happened to the destroyed one.
 */
function destroy_while_waiting(Loop $loop, string $how, Closure $op, array $after = [], array $before = [], array $alongside = []): void
{
    $loop->manage(function () use ($loop, $how, $op, $after, $before, $alongside) {
        $others = [];
        foreach ($before as $c) {
            ($others[] = new Fiber($c))->start();
        }
        $holder = new stdClass;
        $fiber = new Fiber(function () use ($op, $holder) {
            try {
                $r = $op();
                echo "  returned ", json_encode($r), " (not destroyed)\n";
            } finally {
                echo "  finally ran\n";
            }
        });
        if ($how === 'gc') {
            $holder->fiber = $fiber;        // a cycle through the fiber's own frame
        }
        $fiber->start();
        if (!$fiber->isSuspended()) {
            echo "  not suspended\n";
        }
        foreach ($alongside as $c) {
            ($others[] = new Fiber($c))->start();
        }
        $loop->drop($fiber);
        $weak = WeakReference::create($fiber);
        unset($fiber, $holder);
        if ($how === 'gc') {
            gc_collect_cycles();
            echo "  collected: ", $weak->get() === null ? 'yes' : 'no', "\n";
        }
        foreach ($after as $c) {
            $loop->go($c);
        }
        $loop->run();
    });
}
