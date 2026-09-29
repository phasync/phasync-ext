--TEST--
set_preempt_function(): every loop shape is interrupted about every interval, between iterations, straight-line code is not (no JIT)
--EXTENSIONS--
phasync
--INI--
opcache.enable_cli=0
--FILE--
<?php
require __DIR__ . '/preempt_shapes.inc';
?>
--EXPECT--
for: interrupted, mid-iteration: 0
for, calls: interrupted, mid-iteration: 0
while: interrupted, mid-iteration: 0
while, calls: interrupted, mid-iteration: 0
while, calls, if with a call mid-iteration: interrupted, mid-iteration: 0
do-while: interrupted, mid-iteration: 0
do-while, calls: interrupted, mid-iteration: 0
foreach: interrupted, mid-iteration: 0
foreach, calls: interrupted, mid-iteration: 0
while(true), break last: interrupted, mid-iteration: 0
while(true), break last, calls: interrupted, mid-iteration: 0
while(true), break first: interrupted, mid-iteration: 0
while(true), break first, calls: interrupted, mid-iteration: 0
while(true), break last, calls, no C call per iteration (#24): interrupted, mid-iteration: 0
for(;;), break, calls: interrupted, mid-iteration: 0
goto: interrupted, mid-iteration: 0
goto, calls: interrupted, mid-iteration: 0
nested for, calls: interrupted, mid-iteration: 0
nested in while(true), calls: interrupted, mid-iteration: 0
recursion: calls: 0
