--TEST--
set_preempt_function(): loops are interrupted between iterations, code without loops is not (no JIT)
--EXTENSIONS--
phasync
--INI--
opcache.enable_cli=0
--FILE--
<?php require __DIR__ . '/preempt_loops.inc'; ?>
--EXPECT--
-- loops are interrupted between iterations
while: interrupted, mid-iteration: 0
for: interrupted, mid-iteration: 0
do-while: interrupted, mid-iteration: 0
foreach in do-while: interrupted, mid-iteration: 0
while(true) with break: interrupted, mid-iteration: 0
while with continue: interrupted, mid-iteration: 0
foreach with continue: interrupted, mid-iteration: 0
nested for: interrupted, mid-iteration: 0
-- code without a loop is not
recursion: calls: 0
array_map: calls: 0
usort: calls: 0
