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
file_put_contents($child, '<?php require ' . var_export(__DIR__ . '/loop.inc', true) . ';' . <<<'PHP'

// resume ready coroutines in reverse start order: B gets the first turn when the pipe drains
$ticks = 0; $left = 2;
$loop = new Loop;
$loop->reverse = true;
$loop->runAll(
    function () use (&$left) { echo str_repeat('a', 300000); print str_repeat('A', 1000); $left--; },
    function () use (&$left) { echo str_repeat('b', 300000); $left--; },
    function () use (&$ticks, &$left) { while ($left) { usleep(20000); $ticks++; } },
);
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
