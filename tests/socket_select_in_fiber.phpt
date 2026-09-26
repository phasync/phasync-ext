--TEST--
socket_select() inside a scope suspends the fiber and keeps native results/timeouts
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
?>
--FILE--
<?php
// ext/sockets is usually a shared extension that run-tests' `-n` can't load, so
// the body runs in a child PHP with the normal ini and the extension loaded.
$child = tempnam(sys_get_temp_dir(), 'phasync_ss_') . '.php';
file_put_contents($child, <<<'CHILD'
<?php
if (!function_exists('socket_select')) { echo "no sockets\nno sockets\n"; exit; }
final class TestTimeout extends Exception {}
socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $p);
$log = [];
$h = fn($s, $t) => Fiber::suspend($s);
\phasync\ext\manage(function () use ($p, &$log) {
    $a = new Fiber(function () use ($p, &$log) {
        $r = ['k' => $p[0]]; $w = $e = null;
        $n = socket_select($r, $w, $e, 5);          // suspends until B writes
        $log[] = "A: $n " . implode(',', array_keys($r));
    });
    $res = $a->start();                              // parked on an epoll fd
    $log[] = 'B: writes';
    socket_write($p[1], 'x');
    $rr = [$res]; $ww = $ee = null;
    \phasync\ext\stream_select($rr, $ww, $ee, 2);
    $a->resume();
}, $h, $h, fn($us) => null, TestTimeout::class);
echo implode(' | ', $log), "\n";

socket_read($p[0], 1);
$waitReal = function ($s, $t) {
    $r = [$s]; $w = $e = null;
    if (\phasync\ext\stream_select($r, $w, $e, 0, (int) ($t * 1e6)) < 1) throw new TestTimeout();
};
(new Fiber(fn() => \phasync\ext\manage(function () use ($p) {
    $r = [$p[0]]; $w = $e = null;
    printf("timeout: %s r=%d\n", var_export(socket_select($r, $w, $e, 0, 200000), true), count($r));
}, $waitReal, $waitReal, fn($us) => null, TestTimeout::class)))->start();
CHILD);
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' ' . escapeshellarg($child) . ' 2>&1');
unlink($child);
?>
--EXPECT--
B: writes | A: 1 k
timeout: 0 r=0
