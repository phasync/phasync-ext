<?php
use function phasync\ext\set_preempt_function;

// Every loop shape, with and without calls to a function that is hot on its
// own (under the tracing JIT, a loop's trace then links into the function's
// trace and has no check at its head), each run for 0.1 s at an interval of
// 5 ms. $phase is 0 exactly between iterations. The loops end on hrtime(): the
// tracing JIT checks after such internal calls.
function hot(int $x): int { return ($x * 31 + 7) & 0xffffff; }
for ($i = 0, $y = 1; $i < 200000; $i++) { $y = hot($y); }

$phase = 0;
$calls = 0;
$mid = 0;
set_preempt_function(function () use (&$phase, &$calls, &$mid) {
    $calls++;
    if ($phase !== 0) $mid++;
}, 0.005);

const RUN = 100000000;
$shapes = [
    'for' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; for ($i = 0; hrtime(true) < $e; $i++) { $phase = 1; $y = ($y * 31 + 7) & 0xffffff; $phase = 0; } },
    'for, calls' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; for ($i = 0; hrtime(true) < $e; $i++) { $phase = 1; $y = hot($y); $phase = 0; } },
    'while' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; while (hrtime(true) < $e) { $phase = 1; $y = ($y * 31 + 7) & 0xffffff; $phase = 0; } },
    'while, calls' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; while (hrtime(true) < $e) { $phase = 1; $y = hot($y); $phase = 0; } },
    'while, calls, if with a call mid-iteration' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; while (hrtime(true) < $e) { $phase = 1; if (hrtime(true) < 0) break; $y = hot($y); $phase = 0; } },
    'do-while' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; do { $phase = 1; $y = ($y * 31 + 7) & 0xffffff; $phase = 0; } while (hrtime(true) < $e); },
    'do-while, calls' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; do { $phase = 1; $y = hot($y); $phase = 0; } while (hrtime(true) < $e); },
    'foreach' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; $a = range(0, 999); while (hrtime(true) < $e) { foreach ($a as $v) { $phase = 1; $y = ($y * 31 + $v) & 0xffffff; $phase = 0; } } },
    'foreach, calls' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; $a = range(0, 999); while (hrtime(true) < $e) { foreach ($a as $v) { $phase = 1; $y = hot($y + $v); $phase = 0; } } },
    'while(true), break last' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; while (true) { $phase = 1; $y = ($y * 31 + 7) & 0xffffff; $phase = 0; if (hrtime(true) >= $e) break; } },
    'while(true), break last, calls' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; while (true) { $phase = 1; $y = hot($y); $phase = 0; if (hrtime(true) >= $e) break; } },
    'while(true), break first' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; while (true) { if (hrtime(true) >= $e) break; $phase = 1; $y = ($y * 31 + 7) & 0xffffff; $phase = 0; } },
    'while(true), break first, calls' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; while (true) { if (hrtime(true) >= $e) break; $phase = 1; $y = hot($y); $phase = 0; } },
    'while(true), break last, calls, no C call per iteration (#24)' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; $i = 0; while (true) { $phase = 1; $y = hot($y); $phase = 0; if (++$i >= 100000) { if (hrtime(true) >= $e) break; $i = 0; } } },
    'for(;;), break, calls' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; for (;;) { $phase = 1; $y = hot($y); $phase = 0; if (hrtime(true) >= $e) break; } },
    'goto' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; top: $phase = 1; $y = ($y * 31 + 7) & 0xffffff; $phase = 0; if (hrtime(true) < $e) goto top; },
    'goto, calls' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; top: $phase = 1; $y = hot($y); $phase = 0; if (hrtime(true) < $e) goto top; },
    'nested for, calls' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; for ($i = 0; hrtime(true) < $e; $i++) { for ($j = 0; $j < 100; $j++) { $phase = 1; $y = hot($y + $j); $phase = 0; } } },
    'nested in while(true), calls' => function () { global $phase; $y = 1; $e = hrtime(true) + RUN; while (true) { for ($j = 0; $j < 100; $j++) { $phase = 1; $y = hot($y + $j); $phase = 0; } if (hrtime(true) >= $e) break; } },
];
foreach ($shapes as $name => $shape) {
    $calls = $mid = 0;
    $shape();
    echo "$name: ", $calls >= 10 ? "interrupted" : "calls: $calls", ", mid-iteration: $mid\n";
}

function fib(int $n): int { return $n < 2 ? $n : fib($n - 1) + fib($n - 2); }
$n = 20;
do {
    $calls = 0;
    $t = hrtime(true);
    fib($n++);
    $got = $calls;
} while (hrtime(true) - $t < 50000000);
echo "recursion: calls: $got\n";
set_preempt_function(null);
