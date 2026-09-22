--TEST--
fopen() on a regular file POOL-wraps it: fread offloads to the pool and parks the fiber
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
\phasync\enable_hooks();
\phasync\register_read_handler(fn($fd) => Fiber::suspend($fd));

function drive(Fiber $f) {
    $fd = $f->start();
    while (!$f->isTerminated()) {
        $r = [$fd]; $w = $e = null;
        \phasync\stream_select($r, $w, $e, 5);
        $fd = $f->resume();
    }
}

$path = tempnam(sys_get_temp_dir(), 'phasync_pool_');
file_put_contents($path, "regular-file-contents-0123456789");

$f = new Fiber(function () use ($path) {
    $fh = fopen($path, 'r');          // regular file -> POOL-wrapped
    $data = '';
    while (!feof($fh)) {
        $chunk = fread($fh, 8);       // each read offloads to a worker thread
        if ($chunk === '' || $chunk === false) break;
        $data .= $chunk;
    }
    fclose($fh);
    echo "data=$data\n";
});
drive($f);

unlink($path);
echo "done\n";
?>
--EXPECT--
data=regular-file-contents-0123456789
done
