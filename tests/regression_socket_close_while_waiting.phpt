--TEST--
Regression: socket_close() by one coroutine wakes another waiting on the Socket, whose call then fails as on a closed Socket instead of waiting forever (#14)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
// The body runs in a child PHP with the normal ini; skip if that has no ext/sockets.
if (trim((string) shell_exec(escapeshellarg(PHP_BINARY) . ' -r "echo function_exists(\'socket_create\') ? 1 : 0;"')) !== '1') die('skip requires ext/sockets');
?>
--FILE--
<?php
// ext/sockets is usually a shared extension that run-tests' `-n` can't load, so
// the body runs in a child PHP with the normal ini and the extension loaded.
// epoll forgets a closed descriptor, so without a wake-up the reader never returns.
$child = tempnam(sys_get_temp_dir(), 'phasync_sc_') . '.php';
file_put_contents($child, '<?php require ' . var_export(__DIR__ . '/loop.inc', true) . ';' . <<<'CHILD'

socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $p);
(new Loop)->runAll(
    function () use ($p) {
        try { var_dump(socket_read($p[0], 100)); } catch (Error $e) { echo get_class($e), ': ', $e->getMessage(), "\n"; }
    },
    function () use ($p) { usleep(20000); socket_close($p[0]); echo "closed\n"; },
);
CHILD);
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' ' . escapeshellarg($child) . ' 2>&1');
unlink($child);
?>
--EXPECT--
closed
Error: socket_read(): Argument #1 ($socket) has already been closed
