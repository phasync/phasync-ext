--TEST--
Stream hooks do not affect proc_open pipes (only tcp/unix transports are hooked)
--EXTENSIONS--
phasync
--FILE--
<?php
\phasync\enable_hooks();
\phasync\register_read_handler(function($fd){ $r=[$fd];$w=$e=null; \phasync\stream_select($r,$w,$e,2); });
$p = proc_open('printf "proc-output"', [1 => ['pipe','w'], 2 => ['pipe','w']], $pipes);
$out = stream_get_contents($pipes[1]);
fclose($pipes[1]); fclose($pipes[2]);
proc_close($p);
echo $out === 'proc-output' ? "ok\n" : "FAIL: $out\n";
?>
--EXPECT--
ok
