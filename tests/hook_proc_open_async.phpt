--TEST--
proc_open pipe fread suspends a fiber transparently under hooks
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
\phasync\enable_hooks();
\phasync\register_read_handler(fn($fd) => Fiber::suspend(['read', $fd]));

// child sleeps briefly then prints, so the first read would block
$p = proc_open('sh -c "sleep 0.2; printf hello"', [1 => ['pipe','w'], 2 => ['pipe','w']], $pipes);

$fiber = new Fiber(function() use ($pipes) {
    $data = fread($pipes[1], 100);   // no data yet -> suspends transparently
    echo "fiber read: $data\n";
});
$sig = $fiber->start();
echo "suspended: {$sig[0]}\n";

// scheduler: wait for the pipe fd to be readable, then resume
[$type, $fd] = $sig;
$r = [$fd]; $w = $ex = null;
\phasync\stream_select($r, $w, $ex, 2);
$fiber->resume();

fclose($pipes[1]); fclose($pipes[2]);
proc_close($p);
echo "done\n";
?>
--EXPECT--
suspended: read
fiber read: hello
done
