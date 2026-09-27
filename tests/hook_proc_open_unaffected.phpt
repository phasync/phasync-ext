--TEST--
proc_open output is read correctly inside a scope
--EXTENSIONS--
phasync
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
$out = null;
$loop->runAll(function () use (&$out) {
    $p = proc_open('printf "proc-output"', [1 => ['pipe','w'], 2 => ['pipe','w']], $pipes);
    $out = stream_get_contents($pipes[1]);
    fclose($pipes[1]); fclose($pipes[2]);
    proc_close($p);
});
echo $out === 'proc-output' ? "ok\n" : "FAIL: $out\n";
?>
--EXPECT--
ok
