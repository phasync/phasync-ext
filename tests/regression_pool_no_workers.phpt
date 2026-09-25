--TEST--
Regression: if no worker thread can be created, pool ops run inline instead of deadlocking
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
if (trim((string) @shell_exec('id -u')) === '0') die('skip RLIMIT_NPROC is not enforced for root');
if (trim((string) @shell_exec('command -v bash')) === '') die('skip requires bash (ulimit -u)');
?>
--FILE--
<?php
// A child PHP runs under bash `ulimit -u 1` (RLIMIT_NPROC below the user's process
// count), so pthread_create() fails and the worker pool cannot start. A pool op (a
// regular-file read) used to queue its task anyway and wait forever for a worker
// that never existed; it must run inline instead.
$path  = tempnam(sys_get_temp_dir(), 'phasync_nw_');
$child = tempnam(sys_get_temp_dir(), 'phasync_nw_') . '.php';
file_put_contents($path, 'inline-ok');
file_put_contents($child, '<?php
final class TestTimeout extends Exception {}
$called = false;
(new Fiber(fn() => \phasync\ext\manage(function () {
    $fp = fopen(' . var_export($path, true) . ', "r");
    var_dump(fread($fp, 100));
    fclose($fp);
}, function ($s, $t) use (&$called) { $called = true; }, fn($s, $t) => null,
   fn($us) => null, TestTimeout::class)))->start();
var_dump($called);
');

$cmd = 'ulimit -u 1 || exit 3; exec ' . escapeshellarg(PHP_BINARY) . ' -n'
     . ' -d extension_dir=' . escapeshellarg(ini_get('extension_dir'))
     . ' -d extension=phasync ' . escapeshellarg($child);
$p = proc_open(['bash', '-c', $cmd], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
$out = '';
$deadline = microtime(true) + 10;                 // watchdog: a deadlock fails, not hangs
while (!feof($pipes[1]) && microtime(true) < $deadline) {
    $r = [$pipes[1]]; $w = $e = null;
    if (stream_select($r, $w, $e, 0, 200000)) $out .= fread($pipes[1], 8192);
}
if (!feof($pipes[1])) { proc_terminate($p, 9); echo "DEADLOCK\n"; }
echo $out, stream_get_contents($pipes[2]);
fclose($pipes[1]); fclose($pipes[2]);
proc_close($p);
unlink($path); unlink($child);
?>
--EXPECT--
string(9) "inline-ok"
bool(false)
