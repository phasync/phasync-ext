--TEST--
FIFO open() rendezvous across two coroutines via dedicated threads
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (stripos(PHP_OS, 'WIN') === 0) die('skip POSIX FIFO test');
if (!function_exists('posix_mkfifo')) {
    if (trim((string) @shell_exec('command -v mkfifo')) === '') die('skip needs posix_mkfifo or mkfifo(1)');
}
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$fifo = sys_get_temp_dir() . '/phasync_fifo_' . getmypid();
@unlink($fifo);
if (function_exists('posix_mkfifo')) { posix_mkfifo($fifo, 0600); }
else { exec('mkfifo ' . escapeshellarg($fifo)); }

// Each open() blocks until the other end opens: on its own thread, so both
// coroutines can reach the rendezvous.
$data = null;
$loop = new Loop;
$loop->runAll(
    function () use ($fifo, &$data) {
        $fh = fopen($fifo, 'r');
        $data = fread($fh, 100);
        fclose($fh);
    },
    function () use ($fifo) {
        $fh = fopen($fifo, 'w');
        fwrite($fh, "ping");
        fclose($fh);
    },
);
@unlink($fifo);
echo "got: $data\n";
var_dump($loop->parks >= 2);
?>
--EXPECT--
got: ping
bool(true)
