--TEST--
set_preempt_function(): results stay right under the function JIT (php/php-src#23983, #26), and the checkpoint shows in no backtrace
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!extension_loaded('Zend OPcache')) die('skip needs opcache'); ?>
--INI--
opcache.enable=1
opcache.enable_cli=1
opcache.jit=function
opcache.jit_buffer_size=64M
--FILE--
<?php
use function phasync\ext\set_preempt_function;

// php/php-src#23983's function: the closure's calls end its first loop.
function f(&$n) {
    $x = 7;
    for ($i = 1; $n < 20; $i++) {
        $x = ($x * 31 + $i) & 0xffffff;
    }
    $y = 7;
    for ($j = 1; $j < $i; $j++) {
        $y = ($y * 31 + $j) & 0xffffff;
    }
    return $x === $y;
}
function nested($n) { $y = 1; for ($i = 0; $i < $n; $i += 100) { for ($j = 0; $j < 100; $j++) { $y = ($y * 31 + $j) & 0xffffff; } } return $y; }
function loop() { $y = 1; for ($i = 0; ; $i++) { $y = ($y * 31 + $i) & 0xffffff; } }

var_dump(opcache_get_status()['jit']['on']);
$calls = 0;
set_preempt_function(function () use (&$calls) { $calls++; }, 0.002);
var_dump(f($calls), nested(8000000));

$k = 0;
set_preempt_function(function () use (&$k) {
    if (++$k === 1) debug_print_backtrace();
    throw new Exception('from the closure');
}, 0.002);
try {
    loop();
} catch (Exception $e) {
    echo $e->getMessage(), ' at line ', $e->getLine(), "\n", $e->getTraceAsString(), "\n";
}
set_preempt_function(null);
var_dump(function_exists('phasync\ext\checkpoint'));
?>
--EXPECTF--
bool(true)
bool(true)
int(13961473)
#0 %s(%d): {closure%s}()
#1 %s(%d): loop()
from the closure at line %d
#0 %s(%d): {closure%s}()
#1 %s(%d): loop()
#2 {main}
bool(%s)
