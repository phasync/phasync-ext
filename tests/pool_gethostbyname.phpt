--TEST--
Hooked gethostbyname() resolves on the thread pool and parks the fiber
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
\phasync\enable_hooks();
\phasync\register_read_handler(fn($fd) => Fiber::suspend($fd));

/* Minimal single-fiber driver: run until it parks on a pipe fd, wait for that
 * fd (the worker's self-pipe) to become readable, resume, repeat. */
function drive(Fiber $f) {
    $fd = $f->start();
    while (!$f->isTerminated()) {
        $r = [$fd]; $w = $e = null;
        \phasync\stream_select($r, $w, $e, 5);
        $fd = $f->resume();
    }
}

$f = new Fiber(function () {
    $ip = gethostbyname('localhost');
    echo "ip=$ip\n";
});
drive($f);
echo "done\n";
?>
--EXPECT--
ip=127.0.0.1
done
