--TEST--
A coroutine destroyed (by refcount or by the cycle collector) while waiting in sleep(), usleep() or a flock() for a lock another holds ends the wait safely; the lock and the other coroutines keep working
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/destroy.inc';

$file = tempnam(sys_get_temp_dir(), 'phasync_destroy_');
foreach (['refcount', 'gc'] as $how) {
    echo "sleep ($how)\n";
    destroy_while_waiting(new Loop, $how, fn() => sleep(5), after: [function () {
        usleep(1000);
        echo "  then usleep: done\n";
    }]);

    echo "usleep ($how)\n";
    destroy_while_waiting(new Loop, $how, fn() => usleep(5000000), after: [function () {
        echo "  then sleep: ", sleep(0), "\n";
    }]);

    echo "flock ($how)\n";
    $holder = fopen($file, 'r+');
    flock($holder, LOCK_EX);
    $fp = fopen($file, 'r+');
    destroy_while_waiting(new Loop, $how, fn() => flock($fp, LOCK_EX), after: [function () use ($fp, $holder) {
        flock($holder, LOCK_UN);
        echo "  then flock: ", json_encode(flock($fp, LOCK_EX)), "\n";
        flock($fp, LOCK_UN);
    }]);
    fclose($fp);
    fclose($holder);
}
unlink($file);
?>
--EXPECT--
sleep (refcount)
  finally ran
  then usleep: done
usleep (refcount)
  finally ran
  then sleep: 0
flock (refcount)
  finally ran
  then flock: true
sleep (gc)
  finally ran
  collected: yes
  then usleep: done
usleep (gc)
  finally ran
  collected: yes
  then sleep: 0
flock (gc)
  finally ran
  collected: yes
  then flock: true
