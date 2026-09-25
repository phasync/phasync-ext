--TEST--
proc_open pipe fread suspends a fiber with its stream resource under hooks
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
final class TestTimeout extends Exception {}
\phasync\ext\manage(function () {
    $p = proc_open('sh -c "sleep 0.2; printf hello"', [1 => ['pipe','w'], 2 => ['pipe','w']], $pipes);

    $fiber = new Fiber(function() use ($pipes) {
        echo "fiber read: " . fread($pipes[1], 100) . "\n";   // no data yet -> suspends
    });
    $res = $fiber->start();
    var_dump(is_resource($res));

    $r = [$res]; $w = $ex = null;
    \phasync\ext\stream_select($r, $w, $ex, 2);
    $fiber->resume();

    fclose($pipes[1]); fclose($pipes[2]);
    proc_close($p);
    echo "done\n";
},
fn($res) => Fiber::suspend($res),
fn($res) => Fiber::suspend($res),
fn($us) => Fiber::suspend($us),
TestTimeout::class);
?>
--EXPECT--
bool(true)
fiber read: hello
done
