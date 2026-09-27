--TEST--
socket_select() inside a scope suspends the fiber and keeps native results/timeouts
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
// The body runs in a child PHP with the normal ini; skip if that has no ext/sockets.
if (trim((string) shell_exec(escapeshellarg(PHP_BINARY) . ' -r "echo function_exists(\'socket_select\') ? 1 : 0;"')) !== '1') die('skip requires ext/sockets');
?>
--FILE--
<?php
// ext/sockets is usually a shared extension that run-tests' `-n` can't load, so
// the body runs in a child PHP with the normal ini and the extension loaded.
$child = tempnam(sys_get_temp_dir(), 'phasync_ss_') . '.php';
file_put_contents($child, '<?php require ' . var_export(__DIR__ . '/loop.inc', true) . ';' . <<<'CHILD'

socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $p);
$log = [];
(new Loop)->runAll(
    function () use ($p, &$log) {
        $r = ['k' => $p[0]]; $w = $e = null;
        $n = socket_select($r, $w, $e, 5);          // parks until B writes
        $log[] = "A: $n " . implode(',', array_keys($r));
    },
    function () use ($p, &$log) {
        usleep(50000);
        $log[] = 'B: writes';
        socket_write($p[1], 'x');
    },
);
echo implode(' | ', $log), "\n";

socket_read($p[0], 1);
(new Loop)->runAll(function () use ($p) {
    $r = [$p[0]]; $w = $e = null;
    printf("timeout: %s r=%d\n", var_export(socket_select($r, $w, $e, 0, 200000), true), count($r));
});
CHILD);
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' ' . escapeshellarg($child) . ' 2>&1');
unlink($child);
?>
--EXPECT--
B: writes | A: 1 k
timeout: 0 r=0
