--TEST--
A proc_open pipe's real O_NONBLOCK flag is unchanged before, during a park, and after a
cooperative read that must wait (#32)
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!is_dir('/proc/self/fd')) die('skip requires /proc'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
require __DIR__ . '/fdflags.inc';

$p = proc_open('sh -c "sleep 0.2; printf hello"', [1 => ['pipe', 'w']], $pipes);
$fout = fd_for_stream($pipes[1]);

var_dump(real_nonblock($fout));

$loop = new Loop;
$during = null;
$loop->runAll(
    function () use ($pipes) {
        echo "read: " . fread($pipes[1], 5) . "\n";   // nothing yet: parks
    },
    function () use ($fout, &$during) {
        $during = real_nonblock($fout);                // while the reader is parked
    },
);
var_dump($during);
var_dump(real_nonblock($fout));

fclose($pipes[1]);
proc_close($p);
?>
--EXPECT--
bool(false)
read: hello
bool(false)
bool(false)
