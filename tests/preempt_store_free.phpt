--TEST--
set_preempt_function(): loops that store nothing to memory see the flag each iteration under the tracing JIT (no hoisted load)
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!extension_loaded('Zend OPcache')) die('skip needs opcache'); ?>
--INI--
opcache.enable=1
opcache.enable_cli=1
opcache.jit=tracing
opcache.jit_buffer_size=64M
--FILE--
<?php
function tight(int $n): int { $y = 1; for ($i = 0; $i < $n; $i++) { $y = ($y * 31 + $i) & 0xffffff; } return $y; }
function nested(int $n): int { $y = 1; for ($i = 0; $i < $n; $i += 100) { for ($j = 0; $j < 100; $j++) { $y = ($y * 31 + $j) & 0xffffff; } } return $y; }
function dowhile(int $n): int { $y = 1; $i = 0; do { $y = ($y * 31 + $i) & 0xffffff; } while (++$i < $n); return $y; }

$expect = [];
foreach (['tight', 'nested', 'dowhile'] as $f) {
    $expect[$f] = $f(4000000);   // traces compiled with the flag down
}
$calls = 0;
phasync\ext\set_preempt_function(function () use (&$calls) { $calls++; }, 0.001);
foreach (['tight', 'nested', 'dowhile'] as $f) {
    $calls = 0;
    $y = $f(40000000);
    echo $f, ': ', $calls > 0 ? 'called' : 'never called', "\n";
}
phasync\ext\set_preempt_function(null);
foreach (['tight', 'nested', 'dowhile'] as $f) {
    var_dump($f(4000000) === $expect[$f]);
}
?>
--EXPECT--
tight: called
nested: called
dowhile: called
bool(true)
bool(true)
bool(true)
