--TEST--
Hooked gethostbyname() resolves on the thread pool and parks the fiber
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
\phasync\ext\manage(function () {
    // drive a fiber: run until it parks on a pipe fd, wait for that fd (the
    // worker's self-pipe) to become readable, resume, repeat.
    $drive = function (Fiber $f) {
        $fd = $f->start();
        while (!$f->isTerminated()) {
            $r = [$fd]; $w = $e = null;
            \phasync\ext\stream_select($r, $w, $e, 5);
            $fd = $f->resume();
        }
    };
    $f = new Fiber(function () {
        echo "ip=" . gethostbyname('localhost') . "\n";
    });
    $drive($f);
    echo "done\n";
},
fn($fd) => Fiber::suspend($fd),
fn($fd) => Fiber::suspend($fd),
fn($us) => Fiber::suspend($us));
?>
--EXPECT--
ip=127.0.0.1
done
