--TEST--
POOL-mode regular-file reads set EOF exactly like native (feof() trips; no spin)
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
final class TestTimeout extends Exception {}
// A regular file opened inside a scope is POOL-wrapped. A read that returns 0
// bytes must set EOF, just as PHP's native plain-file read does — otherwise
// while (!feof($fp)) fread(...) never terminates. The output below is identical
// to `php -n` on the same file.
$path = tempnam(sys_get_temp_dir(), 'phasync_eof_');
file_put_contents($path, 'Hello, world!');   // 13 bytes

\phasync\ext\manage(function () use ($path) {
    $f = new Fiber(function () use ($path) {
        $fp = fopen($path, 'r');
        var_dump(fread($fp, 65536));   // string(13) "Hello, world!"
        var_dump(feof($fp));           // bool(true)
        var_dump(fread($fp, 65536));   // string(0) ""
        var_dump(feof($fp));           // bool(true)
        fclose($fp);
    });
    $res = $f->start();
    while (!$f->isTerminated()) {
        $r = [$res]; $w = $e = null;
        \phasync\ext\stream_select($r, $w, $e, 5);
        $res = $f->resume();
    }
}, fn($r) => Fiber::suspend($r), fn($w) => Fiber::suspend($w), fn($us) => Fiber::suspend($us), TestTimeout::class);

unlink($path);
?>
--EXPECT--
string(13) "Hello, world!"
bool(true)
string(0) ""
bool(true)
