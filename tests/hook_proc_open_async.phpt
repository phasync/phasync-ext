--TEST--
proc_open pipe fread parks the coroutine inside a scope while others run
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
$ticks = 0; $done = false;
$loop->runAll(
    function () use (&$done) {
        $p = proc_open('sh -c "sleep 0.2; printf hello"', [1 => ['pipe','w'], 2 => ['pipe','w']], $pipes);
        echo "fiber read: " . fread($pipes[1], 100) . "\n";   // no data yet -> parks
        fclose($pipes[1]); fclose($pipes[2]);
        proc_close($p);
        $done = true;
    },
    function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } },
);
var_dump($ticks >= 5);   // others ran while it waited
echo "done\n";
?>
--EXPECT--
fiber read: hello
bool(true)
done
