--TEST--
FIFO open() rendezvous across two fibers via the dedicated-thread pool
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
/*
 * Proof that the thread pool solves what cooperative scheduling cannot:
 * opening a FIFO for reading blocks in the kernel until a writer opens it, and
 * vice versa. With a single main thread, whichever open() runs first would
 * freeze the whole process before the other coroutine could ever run -> deadlock.
 * Offloading each open() to its own OS thread lets the kernel rendezvous complete
 * while the single main thread keeps driving the event loop. The test therefore
 * doubles as a concurrency proof: it can only pass if both opens run at once.
 */
$fifo = sys_get_temp_dir() . '/phasync_fifo_' . getmypid();
@unlink($fifo);
if (function_exists('posix_mkfifo')) {
    posix_mkfifo($fifo, 0600);
} else {
    exec('mkfifo ' . escapeshellarg($fifo));
}

$result = new stdClass();
$result->data = null;

\phasync\ext\manage(function () use ($fifo, $result) {
    $reader = new Fiber(function () use ($fifo, $result) {
        $fh = fopen($fifo, 'r');           // blocks until a writer opens -> own thread
        $result->data = fread($fh, 100);   // after open, FIFO honors EAGAIN (RAW path)
        fclose($fh);
    });
    $writer = new Fiber(function () use ($fifo) {
        $fh = fopen($fifo, 'w');           // blocks until a reader opens -> own thread
        fwrite($fh, "ping");
        fclose($fh);
    });

    // Multi-fiber driver: each fiber parks on a fd; wait for any to be ready,
    // resume it, let it park again, until both finish.
    $pending = [];                          // fd => Fiber
    foreach ([$reader, $writer] as $f) {
        $fd = $f->start();
        if (!$f->isTerminated()) {
            $pending[$fd] = $f;
        }
    }
    while ($pending) {
        $r = array_keys($pending); $w = $e = null;
        \phasync\ext\stream_select($r, $w, $e, 5);
        if (!$r) { echo "timeout\n"; break; }
        foreach ($r as $fd) {
            $f = $pending[$fd];
            unset($pending[$fd]);
            $nfd = $f->resume();
            if (!$f->isTerminated()) {
                $pending[$nfd] = $f;
            }
        }
    }
},
fn($fd) => Fiber::suspend($fd),
fn($fd) => Fiber::suspend($fd),
fn($us) => Fiber::suspend($us));

@unlink($fifo);
echo "got: {$result->data}\n";
echo "done\n";
?>
--EXPECT--
got: ping
done
