--TEST--
set_preempt_function(): disabled with a warning under the function JIT on PHP 8.4+ (php/php-src#23983)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (PHP_VERSION_ID < 80400) die('skip only PHP 8.4+ is affected');
if (!extension_loaded('Zend OPcache')) die('skip needs opcache');
?>
--INI--
opcache.enable=1
opcache.enable_cli=1
opcache.jit=function
opcache.jit_buffer_size=64M
--FILE--
<?php
use function phasync\ext\set_preempt_function;

var_dump(opcache_get_status()["jit"]["on"]);

$calls = 0;
$first = function () use (&$calls) { $calls++; };
var_dump(set_preempt_function($first, 0.005));
$end = microtime(true) + 0.1;
$x = 0;
while (microtime(true) < $end) {
    $x = ($x * 7 + 3) % 1001;
}
echo "calls: $calls\n";
// The closure is kept and returned as usual.
var_dump(@set_preempt_function(null) === $first);
var_dump(set_preempt_function(null));

// A CRTO value with a function-level trigger (T 3: hot counters) is the function JIT too.
ini_set('opcache.jit', '1235');
set_preempt_function($first);
set_preempt_function(null);
?>
--EXPECTF--
bool(true)

Warning: phasync\ext\set_preempt_function(): Preemption is disabled: under opcache's function JIT, PHP 8.4+ can compute wrong results when interrupts are handled in loops (php/php-src#23983) in %s on line %d
NULL
calls: 0
bool(true)
NULL

Warning: phasync\ext\set_preempt_function(): Preemption is disabled: %s in %s on line %d
