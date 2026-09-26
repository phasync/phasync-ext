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
final class TestTimeout extends Exception {}
function run(array $fibers): void {
    $wait = [];
    foreach ($fibers as $k => $f) $wait[$k] = $f->start();
    while ($wait = array_filter($wait)) {
        $r = $w = []; $e = null; $to = 1.0; $now = microtime(true);
        foreach ($wait as $k => [$type, $x]) { if ($type === 'r') $r[$k] = $x; elseif ($type === 'w') $w[$k] = $x; else $to = min($to, max(0, $x - $now)); }
        if ($r || $w) \phasync\ext\stream_select($r, $w, $e, 0, (int) ($to * 1e6)); else usleep((int) ($to * 1e6));
        $now = microtime(true);
        foreach ($wait as $k => [$type, $x]) if (($type === 'r' && isset($r[$k])) || ($type === 'w' && isset($w[$k])) || ($type === 't' && $x <= $now)) $wait[$k] = $fibers[$k]->resume();
    }
}
$h = [fn($s, $t) => Fiber::suspend(['r', $s]), fn($s, $t) => Fiber::suspend(['w', $s]),
      fn($us) => Fiber::suspend(['t', microtime(true) + $us / 1e6]), TestTimeout::class];

$file = tempnam(sys_get_temp_dir(), 'phasync_lock_');
// Each fopen() is its own open file description, so flocks on two of them conflict
// even within this process. The holder releases after 200ms; the waiter must not
// block the ticker meanwhile.
function contend(string $name, Closure $wait): void {
    global $file, $h;
    $holder = fopen($file, 'r+');
    flock($holder, LOCK_EX);
    $res = null; $ticks = 0; $done = false;
    $code = function () use ($wait, $holder, &$res, &$ticks, &$done) {
        run([
            'W' => new Fiber(function () use ($wait, &$res, &$done) { $res = $wait(); $done = true; }),
            'R' => new Fiber(function () use ($holder) { usleep(200000); flock($holder, LOCK_UN); }),
            'T' => new Fiber(function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } }),
        ]);
    };
    \phasync\ext\manage($code, ...$h);
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
(new Fiber(fn() => \phasync\ext\manage($code, ...$h)))->start();
var_dump($out);
flock($holder, LOCK_UN); fclose($holder);

// A handler exception while waiting propagates, and the lock is not taken.
$holder = fopen($file, 'r+'); flock($holder, LOCK_EX);
$fp = fopen($file, 'r+');
$code = function () use ($fp) {
    try { flock($fp, LOCK_EX); echo "acquired?!\n"; } catch (RuntimeException $e) { echo 'caught: ', $e->getMessage(), "\n"; }
};
(new Fiber(fn() => \phasync\ext\manage($code, $h[0], $h[1], fn($us) => throw new RuntimeException('cancelled'), TestTimeout::class)))->start();
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
