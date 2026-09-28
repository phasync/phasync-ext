--TEST--
A regular file opened before manage() is async inside the scope, and native again outside it
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--INI--
phasync.fs_offload=all
--FILE--
<?php
require __DIR__ . '/loop.inc';
$path = tempnam(sys_get_temp_dir(), 'phasync_pre_');
// Three 8 KiB chunks, so each read below has to fetch from the fd (PHP buffers a
// chunk at a time; a small file would be served from the buffer after one read).
file_put_contents($path, str_repeat('A', 8192) . str_repeat('B', 8192) . str_repeat('C', 8192));

$fp = fopen($path, 'r');                      // opened outside any scope
var_dump(fread($fp, 8192) === str_repeat('A', 8192));   // native

$loop = new Loop;
$loop->runAll(fn() => var_dump(fread($fp, 8192) === str_repeat('B', 8192)));
var_dump($loop->parks);                        // int(1): the read went to the pool

var_dump(fread($fp, 8192) === str_repeat('C', 8192));   // native again, position intact
fclose($fp);
unlink($path);
?>
--EXPECT--
bool(true)
bool(true)
int(1)
bool(true)
