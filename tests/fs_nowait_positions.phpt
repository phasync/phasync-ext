--TEST--
phasync.fs_offload=network: plain-file reads in coroutines (RWF_NOWAIT from the page cache) give native data and stream positions
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
$path = tempnam(sys_get_temp_dir(), 'phasync_nowait_');
// 40 000 lines of varied length: about 900 KiB, far past PHP's 8 KiB chunk.
$data = '';
for ($i = 0; $i < 40000; $i++) {
    $data .= "line $i " . str_repeat(chr(65 + $i % 26), $i % 23) . "\n";
}
file_put_contents($path, $data);

$work = function () use ($path) {
    $r = [];
    $fp = fopen($path, 'r');
    $r[] = fread($fp, 5);
    $r[] = ftell($fp);
    $r[] = md5(fread($fp, 20000));
    $r[] = ftell($fp);
    fseek($fp, 123457);
    $r[] = fread($fp, 10);
    $r[] = ftell($fp);
    fseek($fp, -100, SEEK_END);
    $r[] = fread($fp, 1000);
    $r[] = feof($fp);
    $r[] = fread($fp, 1);
    $r[] = feof($fp);
    rewind($fp);
    $n = 0; $h = hash_init('md5');
    while (($l = fgets($fp)) !== false) { $n++; hash_update($h, $l); }
    $r[] = $n; $r[] = hash_final($h); $r[] = ftell($fp);
    fclose($fp);
    $r[] = md5(file_get_contents($path));
    $r[] = file_get_contents($path, false, null, 700000, 30);
    $r[] = count(file($path));
    $r[] = md5_file($path);
    ob_start(); readfile($path); $r[] = md5(ob_get_clean());
    $o = new SplFileObject($path);
    $o->seek(39999);
    $r[] = $o->current();
    // Unbuffered: every fread() is one read of the fd.
    $fp = fopen($path, 'r');
    stream_set_read_buffer($fp, 0);
    fseek($fp, 8190);
    $r[] = fread($fp, 8);
    $r[] = ftell($fp);
    fclose($fp);
    return $r;
};

$native = $work();
$before = $loop->parks;
$in = null;
$loop->runAll(function () use ($work, &$in) { $in = $work(); });
echo 'coroutine: ', $in === $native ? 'same as native' : 'DIFFERENT', ', ', $loop->parks > $before ? 'pooled' : 'inline', "\n";
$before = $loop->parks;
$out = $loop->manage($work);
echo 'no coroutine: ', $out === $native ? 'same as native' : 'DIFFERENT', ', ', $loop->parks > $before ? 'pooled' : 'inline', "\n";
var_dump($native[0], $native[1], $native[3], $native[4], $native[5], $native[8], $native[9], $native[10], $native[12], $native[15]);
unlink($path);
?>
--EXPECT--
coroutine: same as native, inline
no coroutine: same as native, inline
string(5) "line "
int(5)
int(20005)
string(10) "WWWWWW
lin"
int(123467)
string(0) ""
bool(true)
int(40000)
int(908860)
int(40000)
