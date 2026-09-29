--TEST--
A coroutine destroyed (by refcount or by the cycle collector) while a pool thread runs its file read, file write, fsync() or DNS lookup waits for the thread (which writes into its buffers) without suspending; the file and the pool keep working
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--INI--
phasync.fs_offload=all
phasync.thread_pool_size=2
--FILE--
<?php
require __DIR__ . '/destroy.inc';

$file = tempnam(sys_get_temp_dir(), 'phasync_destroy_');
$big = str_repeat('0123456789abcdef', 1 << 20);   // 16 MB: the thread is still at it
file_put_contents($file, $big);
foreach (['refcount', 'gc'] as $how) {
    echo "read ($how)\n";
    $fp = fopen($file, 'r');
    destroy_while_waiting(new Loop, $how, fn() => strlen(fread($fp, strlen($big))), after: [function () use ($fp) {
        rewind($fp);
        echo "  then read: ", fread($fp, 16), "\n";
    }]);
    fclose($fp);

    echo "write ($how)\n";
    $out = tempnam(sys_get_temp_dir(), 'phasync_destroy_');
    $fp = fopen($out, 'w');
    destroy_while_waiting(new Loop, $how, fn() => fwrite($fp, $big), after: [function () use ($fp, $out, $big) {
        clearstatcache();
        echo "  written before: ", filesize($out) === strlen($big) ? 'all' : filesize($out), "\n";
        echo "  then write: ", fwrite($fp, 'tail'), "\n";
    }]);
    fclose($fp);
    unlink($out);

    echo "fsync ($how)\n";
    $fp = fopen($file, 'r+');
    destroy_while_waiting(new Loop, $how, fn() => fsync($fp), after: [function () use ($fp) {
        echo "  then fsync: ", json_encode(fsync($fp)), "\n";
    }]);
    fclose($fp);

    echo "gethostbyname ($how)\n";
    destroy_while_waiting(new Loop, $how, fn() => gethostbyname('localhost'), after: [function () {
        echo "  then: ", gethostbyname('127.0.0.1'), "\n";
    }]);
}
unlink($file);
?>
--EXPECT--
read (refcount)
  finally ran
  then read: 0123456789abcdef
write (refcount)
  finally ran
  written before: all
  then write: 4
fsync (refcount)
  finally ran
  then fsync: true
gethostbyname (refcount)
  finally ran
  then: 127.0.0.1
read (gc)
  finally ran
  collected: yes
  then read: 0123456789abcdef
write (gc)
  finally ran
  collected: yes
  written before: all
  then write: 4
fsync (gc)
  finally ran
  collected: yes
  then fsync: true
gethostbyname (gc)
  finally ran
  collected: yes
  then: 127.0.0.1
