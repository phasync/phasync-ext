--TEST--
flock()/file_put_contents(LOCK_EX)/SplFileObject::flock() wait for a held lock cooperatively inside a scope, with native results
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';

$file = tempnam(sys_get_temp_dir(), 'phasync_lock_');
// Each fopen() is its own open file description, so flocks on two of them conflict
// even within this process. The holder releases after 200ms; the waiter must not
// block the ticker meanwhile.
function contend(string $name, Closure $wait): void {
    global $file;
    $holder = fopen($file, 'r+');
    flock($holder, LOCK_EX);
    $res = null; $ticks = 0; $done = false;
    (new Loop)->runAll(
        function () use ($wait, &$res, &$done) { $res = $wait(); $done = true; },
        function () use ($holder) { usleep(200000); flock($holder, LOCK_UN); },
        function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } },
    );
    fclose($holder);
    printf("%-26s %s %s\n", $name, json_encode($res), $ticks >= 5 ? 'cooperative' : "BLOCKED (ticks=$ticks)");
}

contend('flock LOCK_EX', function () use ($file) { $fp = fopen($file, 'r+'); $r = flock($fp, LOCK_EX); fclose($fp); return $r; });
contend('flock LOCK_SH', function () use ($file) { $fp = fopen($file, 'r'); $r = flock($fp, LOCK_SH); fclose($fp); return $r; });
contend('file_put_contents LOCK_EX', fn() => [file_put_contents($file, 'data', LOCK_EX), file_get_contents($file)]);
contend('SplFileObject::flock', function () use ($file) { $f = new SplFileObject($file, 'r+'); return $f->flock(LOCK_EX); });

// Non-blocking and shared-on-shared requests stay immediate and native.
$holder = fopen($file, 'r+'); flock($holder, LOCK_EX);
$code = function () use ($file, &$out) {
    $fp = fopen($file, 'r+');
    $out = [flock($fp, LOCK_EX | LOCK_NB, $wb), $wb];
};
$out = null;
(new Loop)->runAll($code);
var_dump($out);
flock($holder, LOCK_UN); fclose($holder);

// Cancelling the waiter propagates the cancellation, and the lock is not taken.
$holder = fopen($file, 'r+'); flock($holder, LOCK_EX);
$fp = fopen($file, 'r+');
$loop = new Loop;
$loop->runAll(
    function () use ($fp, $loop) {
        $GLOBALS['waiter'] = Fiber::getCurrent();
        try { flock($fp, LOCK_EX); echo "acquired?!\n"; } catch (LoopCancelled $e) { echo 'caught: ', $e->getMessage(), "\n"; }
    },
    function () use ($loop) { usleep(100000); $loop->cancel($GLOBALS['waiter']); },
);
flock($holder, LOCK_UN); fclose($holder);
$probe = fopen($file, 'r+');
var_dump(flock($probe, LOCK_EX | LOCK_NB));   // free: $fp never got it
fclose($probe); fclose($fp);
unlink($file);
?>
--EXPECT--
flock LOCK_EX              true cooperative
flock LOCK_SH              true cooperative
file_put_contents LOCK_EX  [4,"data"] cooperative
SplFileObject::flock       true cooperative
array(2) {
  [0]=>
  bool(false)
  [1]=>
  int(1)
}
caught: cancelled
bool(true)
