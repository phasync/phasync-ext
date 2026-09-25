--TEST--
Regression: a fatal error inside the wait handler reaps the worker thread before unwinding (no stack-lifetime hole)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('posix_mkfifo') && trim((string) @shell_exec('command -v mkfifo')) === '') die('skip needs posix_mkfifo or mkfifo(1)');
if (!is_readable('/proc/self/status')) die('skip requires /proc');
?>
--FILE--
<?php
// A FIFO open() runs on a dedicated thread holding a pointer to a task in the
// caller's C frame. A fatal error (bailout) raised inside the handler longjmps
// past that frame; the extension must cancel and join the thread first. Observable:
// after the fatal, only the main thread is left.
final class TestTimeout extends Exception {}
$fifo = sys_get_temp_dir() . '/phasync_bail_' . getmypid();
function_exists('posix_mkfifo') ? posix_mkfifo($fifo, 0600) : shell_exec('mkfifo ' . escapeshellarg($fifo));

register_shutdown_function(function () use ($fifo) {
    preg_match('/^Threads:\s+(\d+)/m', file_get_contents('/proc/self/status'), $m);
    echo "threads=", $m[1], "\n";
    @unlink($fifo);
});

(new Fiber(fn() => \phasync\ext\manage(function () use ($fifo) {
    fopen($fifo, 'r');                         // no writer: the thread blocks in open()
}, function ($s, $t) {
    ini_set('memory_limit', '8M');
    $x = str_repeat('x', 32 << 20);            // fatal error inside the handler
}, fn($s, $t) => null, fn($us) => null, TestTimeout::class)))->start();
?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%s
threads=1
