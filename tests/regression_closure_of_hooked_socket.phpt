--TEST--
A closure of a hooked ext/sockets function (socket_read(...), Closure::fromCallable(), array_map()) works in and out of a coroutine (#9)
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
$child = tempnam(sys_get_temp_dir(), 'phasync_cs_') . '.php';
file_put_contents($child, '<?php require ' . var_export(__DIR__ . '/loop.inc', true) . ';' . <<<'CHILD'

function run(): array {
    socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $p);
    $write = socket_write(...);
    $read = Closure::fromCallable('socket_read');
    return [
        $write($p[1], 'ping'),
        $read($p[0], 100),
        array_map(socket_send(...), [$p[0]], ['pong'], [4], [0]),
        array_map(fn($s) => socket_read($s, 100), [$p[1]]),
    ];
}

echo 'outside: ', json_encode(run()), "\n";
$in = null;
(new Loop)->runAll(function () use (&$in) { $in = run(); });
echo 'inside:  ', json_encode($in), "\n";
CHILD);
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' ' . escapeshellarg($child) . ' 2>&1');
unlink($child);
?>
--EXPECT--
outside: [4,"ping",[4],["pong"]]
inside:  [4,"ping",[4],["pong"]]
