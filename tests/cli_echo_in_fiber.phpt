--TEST--
echo to a full stdout pipe suspends the fiber inside a scope (CLI), keeps each echo contiguous, and loses nothing
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
if (PHP_SAPI !== 'cli') die('skip CLI only');
?>
--FILE--
<?php
// The child's stdout is a pipe the parent leaves unread for 300ms, so both
// fibers' 300 KB echoes fill it; the ticker must keep running meanwhile.
$child = tempnam(sys_get_temp_dir(), 'phasync_echo_') . '.php';
file_put_contents($child, <<<'PHP'
<?php
final class T extends Exception {}
function run(array $fibers): void {
    $wait = [];
    foreach ($fibers as $k => $f) $wait[$k] = $f->start();
    while ($wait = array_filter($wait)) {
        $r = $w = []; $e = null; $to = 1.0; $now = microtime(true);
        foreach ($wait as $k => [$type, $x]) { if ($type === 'r') $r[$k] = $x; elseif ($type === 'w') $w[$k] = $x; else $to = min($to, max(0, $x - $now)); }
        if ($r || $w) \phasync\ext\stream_select($r, $w, $e, 0, (int) ($to * 1e6)); else usleep((int) ($to * 1e6));
        $now = microtime(true);
        // resume in reverse start order: B gets the first turn when the pipe drains
        foreach (array_reverse($wait, true) as $k => [$type, $x]) if (($type === 'r' && isset($r[$k])) || ($type === 'w' && isset($w[$k])) || ($type === 't' && $x <= $now)) $wait[$k] = $fibers[$k]->resume();
    }
}
$ticks = 0; $left = 2;
\phasync\ext\manage(function () use (&$ticks, &$left) {
    run([
        'A' => new Fiber(function () use (&$left) { echo str_repeat('a', 300000); print str_repeat('A', 1000); $left--; }),
        'B' => new Fiber(function () use (&$left) { echo str_repeat('b', 300000); $left--; }),
        'T' => new Fiber(function () use (&$ticks, &$left) { while ($left) { usleep(20000); $ticks++; } }),
    ]);
}, fn($s, $t) => Fiber::suspend(['r', $s]), fn($s, $t) => Fiber::suspend(['w', $s]),
   fn($us) => Fiber::suspend(['t', microtime(true) + $us / 1e6]), T::class);
fwrite(STDERR, $ticks >= 5 ? "cooperative\n" : "BLOCKED (ticks=$ticks)\n");
PHP);
$cmd = [PHP_BINARY, '-n', '-d', 'extension_dir=' . ini_get('extension_dir'), '-d', 'extension=phasync', $child];
$p = proc_open($cmd, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
usleep(300000);
$out = stream_get_contents($pipes[1]);
echo stream_get_contents($pipes[2]);
proc_close($p);
unlink($child);
// Each echo is one contiguous run, in some order, and nothing is lost.
$runs = preg_split('/(?<=(.))(?!\1)/', $out, -1, PREG_SPLIT_NO_EMPTY);
$runs = array_map(fn($r) => $r[0] . strlen($r), $runs);
sort($runs);
echo implode(' ', $runs), "\n";
?>
--EXPECT--
cooperative
A1000 a300000 b300000
