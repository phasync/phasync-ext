--TEST--
A descriptor shared with a child process keeps its real O_NONBLOCK flag: the child observes
its inherited copy unchanged while the parent is parked waiting on its own end (#32)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
if (!is_dir('/proc/self/fd')) die('skip requires /proc');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
require __DIR__ . '/fdflags.inc';

$child = tempnam(sys_get_temp_dir(), 'phasync_shared_') . '.php';
file_put_contents($child, <<<'PHP'
<?php
usleep(200000);
$info = file_get_contents('/proc/self/fdinfo/4');
preg_match('/^flags:\s*(\d+)$/m', $info, $m);
$nonblock = (octdec($m[1]) & 04000) !== 0;
fwrite(fopen('php://fd/3', 'w'), $nonblock ? "child: nonblocking\n" : "child: blocking\n");
fwrite(fopen('php://fd/4', 'r+'), 'hello');
PHP);

[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
$fa = fd_for_stream($a);

$p = proc_open([PHP_BINARY, '-n', $child], [3 => ['pipe', 'w'], 4 => $b], $pipes);
fclose($b);

var_dump(real_nonblock($fa));

$loop = new Loop;
$ticks = 0; $done = false;
$loop->runAll(
    function () use ($a, &$done) {
        echo "parent read: " . fread($a, 5) . "\n";   // parks until the child writes
        $done = true;
    },
    function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } },
);
var_dump($ticks >= 3);                 // other coroutines ran while parked
var_dump(real_nonblock($fa));          // unchanged after

echo trim(stream_get_contents($pipes[3])), "\n";

fclose($a);
fclose($pipes[3]);
proc_close($p);
unlink($child);
?>
--EXPECT--
bool(false)
parent read: hello
bool(true)
bool(false)
child: blocking
