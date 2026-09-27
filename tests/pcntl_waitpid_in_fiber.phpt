--TEST--
pcntl_waitpid()/pcntl_wait() wait for a child's exit cooperatively inside a scope; WNOHANG and ECHILD stay native; a cancelled wait reaps nothing
--EXTENSIONS--
phasync
pcntl
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';

function child(float $seconds, int $code): int {
    $pid = pcntl_fork();
    if ($pid === 0) { usleep((int) ($seconds * 1e6)); exit($code); }
    return $pid;
}
// $op runs in a coroutine beside a ticker.
function coop(string $name, Closure $op): mixed {
    $ticks = 0; $done = false; $res = null;
    (new Loop)->runAll(
        function () use ($op, &$done, &$res) { $res = $op(); $done = true; },
        function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } },
    );
    printf("%-22s %s\n", $name, $ticks >= 5 ? 'cooperative' : "BLOCKED (ticks=$ticks)");
    return $res;
}

// A given pid: same result and exit status as native.
$pid = child(0.3, 7);
$r = coop('pcntl_waitpid($pid)', function () use ($pid) { $got = pcntl_waitpid($pid, $st); return [$got === $pid, pcntl_wexitstatus($st)]; });
var_dump($r);

// Any child: pcntl_waitpid(-1) and pcntl_wait().
$pid = child(0.3, 3);
$r = coop('pcntl_waitpid(-1)', function () use ($pid) { $got = pcntl_waitpid(-1, $st); return [$got === $pid, pcntl_wexitstatus($st)]; });
var_dump($r);
$pid = child(0.3, 4);
$r = coop('pcntl_wait()', function () use ($pid) { $got = pcntl_wait($st); return [$got === $pid, pcntl_wexitstatus($st)]; });
var_dump($r);

// WNOHANG never waits; with no children at all, -1 (ECHILD) as natively.
$pid = child(0.3, 0);
$l = new Loop;
$r = null;
$l->runAll(function () use ($pid, &$r) { $r = [pcntl_waitpid($pid, $st, WNOHANG), pcntl_waitpid($pid, $st)]; });
var_dump($r[0], $r[1] === $pid);
$l = new Loop;
$l->runAll(function () use (&$r) { $r = [pcntl_waitpid(-1, $st), pcntl_get_last_error() === PCNTL_ECHILD]; });
var_dump($r);

// A cancelled wait reaps nothing: the child and its status are still there after.
$pid = child(0.3, 9);
$l = new Loop;
$l->runAll(
    function () use ($pid) {
        $GLOBALS['w'] = Fiber::getCurrent();
        try { pcntl_waitpid($pid, $st); echo "reaped?!\n"; } catch (LoopCancelled $e) { echo "wait cancelled\n"; }
    },
    function () use ($l) { usleep(50000); $l->cancel($GLOBALS['w']); },
);
var_dump(pcntl_waitpid($pid, $st) === $pid, pcntl_wexitstatus($st));
?>
--EXPECT--
pcntl_waitpid($pid)    cooperative
array(2) {
  [0]=>
  bool(true)
  [1]=>
  int(7)
}
pcntl_waitpid(-1)      cooperative
array(2) {
  [0]=>
  bool(true)
  [1]=>
  int(3)
}
pcntl_wait()           cooperative
array(2) {
  [0]=>
  bool(true)
  [1]=>
  int(4)
}
int(0)
bool(true)
array(2) {
  [0]=>
  int(-1)
  [1]=>
  bool(true)
}
wait cancelled
bool(true)
int(9)
