--TEST--
swap_symbols() swaps the global symbol table (isolate + restore)
--EXTENSIONS--
phasync
--FILE--
<?php
// Run inside a function so the handle locals ($saved/$mine) live in the frame's
// CV space, not in the global table we are swapping.
function run(): void
{
    $GLOBALS['x'] = 'main';
    var_dump($GLOBALS['x'] ?? null);              // main

    $saved = \phasync\ext\swap_symbols(0);        // install a fresh empty scope
    var_dump($GLOBALS['x'] ?? null);              // NULL — isolated
    $GLOBALS['x'] = 'inner';
    var_dump($GLOBALS['x']);                      // inner (in the isolated scope)

    $mine = \phasync\ext\swap_symbols($saved);    // restore the caller's scope
    var_dump($GLOBALS['x']);                      // main — restored
    var_dump($GLOBALS['x'] === 'main' && !isset($leaked)); // inner value did not leak
    \phasync\ext\free_symbols($mine);             // discard the isolated scope

    try {
        \phasync\ext\swap_symbols(424242);        // stale/unknown handle
    } catch (\ValueError $e) {
        echo "ValueError\n";
    }
    echo "done\n";
}
run();
?>
--EXPECT--
string(4) "main"
NULL
string(5) "inner"
string(4) "main"
bool(true)
ValueError
done
