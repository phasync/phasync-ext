--TEST--
set_preempt_function(): interval, removal, previous, exceptions, suspension (tracing JIT)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!is_dir('/proc/self/task')) die('skip needs /proc');
if (!extension_loaded('Zend OPcache')) die('skip needs opcache');
?>
--INI--
opcache.enable=1
opcache.enable_cli=1
opcache.jit=tracing
opcache.jit_buffer_size=64M
--FILE--
<?php
// Not opcache.jit=function: on PHP 8.4+ it computes wrong results in code that
// handles interrupts, pcntl's as well (php/php-src#23983).
var_dump(opcache_get_status()['jit']['on']);
require __DIR__ . '/preempt_basic.inc';
?>
--EXPECT--
bool(true)
-- none set
NULL
bool(true)
-- about every interval
NULL
bool(true)
ok
-- loop without calls
bool(true)
-- returns previous, null removes
bool(true)
bool(true)
NULL
bool(true)
int(0)
-- exception at the interrupted point
array(3) {
  [0]=>
  string(9) "preempted"
  [1]=>
  string(4) "spin"
  [2]=>
  bool(true)
}
caught in caller: preempted
-- suspend from the closure, resume into the loop
string(9) "preempted"
string(7) "resumed"
string(9) "preempted"
string(7) "resumed"
string(9) "preempted"
string(7) "resumed"
string(9) "preempted"
string(7) "resumed"
string(9) "preempted"
string(7) "resumed"
bool(true)
int(5)
bool(true)
-- bad interval
phasync\ext\set_preempt_function(): Argument #2 ($minInterval) must be a positive number of seconds
phasync\ext\set_preempt_function(): Argument #2 ($minInterval) must be a positive number of seconds
phasync\ext\set_preempt_function(): Argument #2 ($minInterval) must be a positive number of seconds
phasync\ext\set_preempt_function(): Argument #2 ($minInterval) must be a positive number of seconds
NULL
