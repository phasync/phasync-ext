--TEST--
Regression: php://temp/memory streams in a fiber inside a scope: stream_select() casts and >2MB spills keep working (no segfault, data intact)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
?>
--FILE--
<?php
// Casting php://temp to an fd spills its buffer into a temporary stdio file,
// written inside the cast; the file must stay a plain stdio stream (php://temp
// checks php_stream_is(STDIO) on it) or the next cast crashed. Reported with
// phasync::io(), whose driver passes such streams to phasync\ext\stream_select().
final class TestTimeout extends Exception {}
$wait = function ($s, $t) { $r = [$s]; $w = $e = null; \phasync\ext\stream_select($r, $w, $e, 5); };
$code = function () {
    $t = fopen('php://temp', 'w+');
    fwrite($t, 'hello');
    for ($i = 0; $i < 3; $i++) {                 // repeated casts
        $r = [$t]; $w = [$t]; $e = null;
        echo 'ext select: ', \phasync\ext\stream_select($r, $w, $e, 0), "\n";
    }
    $r = [$t]; $w = []; $e = null;
    echo 'native select: ', stream_select($r, $w, $e, 0), "\n";
    rewind($t);
    echo 'data: ', stream_get_contents($t), "\n";

    $m = fopen('php://memory', 'w+');            // can't be cast: false, no crash
    $r = [$m]; $w = $e = null;
    var_dump(@\phasync\ext\stream_select($r, $w, $e, 0));

    $big = fopen('php://temp/maxmemory:1024', 'w+');   // spills on write
    fwrite($big, str_repeat('x', 5000));
    fwrite($big, 'end');
    rewind($big);
    echo 'spilled: ', strlen(stream_get_contents($big)), "\n";
};
(new Fiber(fn() => \phasync\ext\manage($code, $wait, $wait, fn($us) => null, TestTimeout::class)))->start();
?>
--EXPECT--
ext select: 2
ext select: 2
ext select: 2
native select: 1
data: hello
bool(false)
spilled: 5003
