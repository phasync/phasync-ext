--TEST--
phasync.fs_offload=network: a read that would wait for the disk (RWF_NOWAIT says EAGAIN) goes to the pool, whole or its uncached remainder; a cached one reads inline
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
// Needs a disk filesystem with RWF_NOWAIT (tmpfs never waits; ZFS, overlayfs
// ... refuse it) and GNU dd to drop a file from the page cache (fadvise).
$dev = stat(sys_get_temp_dir())['dev'];
$type = null;
foreach (file('/proc/self/mountinfo') as $l) {
    [$pre, $post] = explode(' - ', $l, 2);
    [$maj, $min] = explode(':', explode(' ', $pre)[2]);
    if ((($maj << 8) | ($min & 0xff) | (($min & ~0xff) << 12)) === $dev) $type = strtok($post, ' ');
}
if (!in_array($type, ['ext4', 'xfs', 'btrfs', 'f2fs'], true)) die("skip the temp dir is on $type, not a disk filesystem known to support RWF_NOWAIT");
exec('dd if=/dev/null iflag=nocache count=0 status=none 2>&1', $o, $rc);
if ($rc !== 0) die('skip needs GNU dd (iflag=nocache)');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
$path = tempnam(sys_get_temp_dir(), 'phasync_cold_');
$data = random_bytes(1 << 20);
$fp = fopen($path, 'w');
fwrite($fp, $data);
fsync($fp);                              // clean pages can be dropped, dirty ones not
fclose($fp);
// Drop the file (or 4 KiB blocks $skip.. of it) from the page cache.
$drop = function (int $skip = 0) use ($path) {
    exec('dd if=' . escapeshellarg($path) . " iflag=nocache bs=4096 skip=$skip count=0 status=none", $o, $rc);
    return $rc === 0;
};

$loop->runAll(function () use ($loop, $path, $data, $drop) {
    $drop();
    $p = $loop->parks;
    $got = file_get_contents($path);
    echo 'cold file_get_contents: ', $got === $data ? 'ok' : 'WRONG', ', ', $loop->parks > $p ? 'pooled' : 'inline', "\n";
    $p = $loop->parks;
    $got = file_get_contents($path);
    echo 'warm file_get_contents: ', $got === $data ? 'ok' : 'WRONG', ', ', $loop->parks > $p ? 'pooled' : 'inline', "\n";

    // The first 68 KiB cached, the rest not: the 8 KiB chunk across the edge
    // reads its first half inline (a short read) and the rest on the pool.
    $drop(17);
    $fp = fopen($path, 'r');
    $p = $loop->parks;
    $got = fread($fp, 1 << 20);
    echo 'half-cold fread: ', $got === $data ? 'ok' : 'WRONG', ', ', $loop->parks > $p ? 'pooled' : 'inline', ', at ', ftell($fp), "\n";
    fclose($fp);

    // Cold from 512 KiB: a read across it after fseek() keeps positions right.
    $drop(128);
    $fp = fopen($path, 'r');
    $p = $loop->parks;
    fseek($fp, 300000);
    $got = fread($fp, 300000);
    echo 'fseek+fread across the cold part: ', $got === substr($data, 300000, 300000) ? 'ok' : 'WRONG', ', ', $loop->parks > $p ? 'pooled' : 'inline', ', at ', ftell($fp), "\n";
    fclose($fp);
});

// Outside a coroutine a cold read is native.
$drop();
$p = $loop->parks;
$got = $loop->manage(fn () => file_get_contents($path));
echo 'no coroutine, cold: ', $got === $data ? 'ok' : 'WRONG', ', ', $loop->parks > $p ? 'pooled' : 'inline', "\n";
unlink($path);
?>
--EXPECT--
cold file_get_contents: ok, pooled
warm file_get_contents: ok, inline
half-cold fread: ok, pooled, at 1048576
fseek+fread across the cold part: ok, pooled, at 600000
no coroutine, cold: ok, inline
