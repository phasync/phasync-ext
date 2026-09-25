--TEST--
A regular file opened before manage() is async inside the scope, and native again outside it
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
final class TestTimeout extends Exception {}
$path = tempnam(sys_get_temp_dir(), 'phasync_pre_');
// Three 8 KiB chunks, so each read below has to fetch from the fd (PHP buffers a
// chunk at a time; a small file would be served from the buffer after one read).
file_put_contents($path, str_repeat('A', 8192) . str_repeat('B', 8192) . str_repeat('C', 8192));

$fp = fopen($path, 'r');                      // opened outside any scope
var_dump(fread($fp, 8192) === str_repeat('A', 8192));   // native

$waits = 0;
\phasync\ext\manage(function () use ($fp) {
    $f = new Fiber(fn() => var_dump(fread($fp, 8192) === str_repeat('B', 8192)));
    $res = $f->start();                       // the read went to the pool and parked
    var_dump(is_resource($res));              // bool(true): parked on the pool's pipe
    $r = [$res]; $w = $e = null;
    \phasync\ext\stream_select($r, $w, $e, 5);
    $f->resume();
}, function ($s, $t) use (&$waits) { $waits++; return Fiber::suspend($s); },
   fn($s, $t) => null, fn($us) => null, TestTimeout::class);

var_dump($waits);                              // int(1)
var_dump(fread($fp, 8192) === str_repeat('C', 8192));   // native again, position intact
fclose($fp);
unlink($path);
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
int(1)
bool(true)
