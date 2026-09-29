--TEST--
set_preempt_function(): every loop shape is interrupted about every interval, between iterations, straight-line code is not (function JIT)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!extension_loaded('Zend OPcache')) die('skip needs opcache');
if (PHP_VERSION_ID >= 80400) die('skip preemption is disabled under the function JIT on 8.4+ (php/php-src#23983)');
?>
--INI--
opcache.enable=1
opcache.enable_cli=1
opcache.jit=function
opcache.jit_buffer_size=64M
--FILE--
<?php
var_dump(opcache_get_status()['jit']['on']);
require __DIR__ . '/preempt_shapes.inc';
?>
--EXPECT--
bool(true)
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
for(;;), break, calls: interrupted, mid-iteration: 0
goto: interrupted, mid-iteration: 0
goto, calls: interrupted, mid-iteration: 0
nested for, calls: interrupted, mid-iteration: 0
nested in while(true), calls: interrupted, mid-iteration: 0
recursion: calls: 0
