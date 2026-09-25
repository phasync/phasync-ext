--TEST--
Hooked gethostbyname() resolves on the thread pool; handler gets a stream resource
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
final class TestTimeout extends Exception {}
\phasync\ext\manage(function () {
    // Drive a fiber: it parks by suspending the RESOURCE the handler was given
    // (the worker's completion pipe, wrapped as a stream); wait on that resource
    // via the fd-capable stream_select, resume, repeat.
    $drive = function (Fiber $f) {
        $res = $f->start();
        while (!$f->isTerminated()) {
            var_dump(is_resource($res));                 // handler received a resource
            $r = [$res]; $w = $e = null;
            \phasync\ext\stream_select($r, $w, $e, 5);
            $res = $f->resume();
        }
    };
    $f = new Fiber(function () {
        echo "ip=" . gethostbyname('localhost') . "\n";
    });
    $drive($f);
    echo "done\n";
},
fn($res) => Fiber::suspend($res),
fn($res) => Fiber::suspend($res),
fn($us) => Fiber::suspend($us),
TestTimeout::class);
?>
--EXPECT--
bool(true)
ip=127.0.0.1
done
