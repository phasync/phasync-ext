--TEST--
FIFO open() rendezvous across two fibers via the dedicated-thread pool (resource handlers)
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
$fifo = sys_get_temp_dir() . '/phasync_fifo_' . getmypid();
@unlink($fifo);
if (function_exists('posix_mkfifo')) { posix_mkfifo($fifo, 0600); }
else { exec('mkfifo ' . escapeshellarg($fifo)); }

$result = new stdClass();
$result->data = null;

\phasync\ext\manage(function () use ($fifo, $result) {
    $reader = new Fiber(function () use ($fifo, $result) {
        $fh = fopen($fifo, 'r');           // blocks until a writer opens -> own thread
        $result->data = fread($fh, 100);
        fclose($fh);
    });
    $writer = new Fiber(function () use ($fifo) {
        $fh = fopen($fifo, 'w');           // blocks until a reader opens -> own thread
        fwrite($fh, "ping");
        fclose($fh);
    });

    // Each fiber parks by suspending a resource; drive them together with one
    // stream_select over all pending resources, until both finish.
    $pending = [];                          // resource-id => [Fiber, resource]
    foreach ([$reader, $writer] as $f) {
        $res = $f->start();
        if (!$f->isTerminated()) { $pending[(int)$res] = [$f, $res]; }
    }
    while ($pending) {
        $r = array_map(fn($p) => $p[1], $pending); $w = $e = null;
        \phasync\ext\stream_select($r, $w, $e, 5);
        if (!$r) { echo "timeout\n"; break; }
        foreach ($r as $res) {
            [$f] = $pending[(int)$res];
            unset($pending[(int)$res]);
            $nres = $f->resume();
            if (!$f->isTerminated()) { $pending[(int)$nres] = [$f, $nres]; }
        }
    }
},
fn($res) => Fiber::suspend($res),
fn($res) => Fiber::suspend($res),
fn($us) => Fiber::suspend($us));

@unlink($fifo);
echo "got: {$result->data}\n";
echo "done\n";
?>
--EXPECT--
got: ping
done
