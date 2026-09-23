--TEST--
proc_open output is read correctly under active hooks
--EXTENSIONS--
phasync
--FILE--
<?php
$mini = function($fd){ $r=[$fd]; $w=$e=null; \phasync\ext\stream_select($r,$w,$e,2); };
$out = \phasync\ext\manage(function () {
    $p = proc_open('printf "proc-output"', [1 => ['pipe','w'], 2 => ['pipe','w']], $pipes);
    $out = stream_get_contents($pipes[1]);
    fclose($pipes[1]); fclose($pipes[2]);
    proc_close($p);
    return $out;
}, $mini, $mini, fn($us) => null);
echo $out === 'proc-output' ? "ok\n" : "FAIL: $out\n";
?>
--EXPECT--
ok
