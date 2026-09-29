--TEST--
A write to a full blocking pipe parks cooperatively (other coroutines run meanwhile) and
completes in full once the reader drains it, with the real O_NONBLOCK flag unchanged
throughout (only ever touched, and restored, around the single non-blocking attempt) (#32)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
if (!is_dir('/proc/self/fd')) die('skip requires /proc');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
require __DIR__ . '/fdflags.inc';

// A payload well past OS pipe capacity (64KB, typically), and a reader that only starts
// draining well after the writer must have filled it and parked.
$p = proc_open([PHP_BINARY, '-n', '-r',
    'usleep(300000); $t = 0; while (($d = fread(STDIN, 65536)) !== "") { $t += strlen($d); } echo $t;'],
    [0 => ['pipe', 'r'], 1 => ['pipe', 'w']], $pipes);
$w = $pipes[0];
$fw = fd_for_stream($w);

var_dump(real_nonblock($fw));

$payload = str_repeat('y', 300000);
$loop = new Loop;
$ticks = 0; $done = false; $written = null; $samples = [];
$loop->runAll(
    function () use ($w, $payload, &$done, &$written) {
        $written = fwrite($w, $payload);
        $done = true;
    },
    function () use ($fw, &$ticks, &$done, &$samples) {
        while (!$done) {
            $samples[] = real_nonblock($fw);
            usleep(20000);
            $ticks++;
        }
    },
);

var_dump($ticks >= 3);                       // other coroutines ran while it waited
var_dump($written === strlen($payload));     // the write completed in full
var_dump(!in_array(true, $samples, true));   // never observed non-blocking
var_dump(real_nonblock($fw));                // unchanged after
fclose($w);                                  // EOF: lets the child's read loop finish

var_dump(trim(stream_get_contents($pipes[1])) === (string) strlen($payload));  // reader got it all
fclose($pipes[1]);
proc_close($p);
?>
--EXPECT--
bool(false)
bool(true)
bool(true)
bool(true)
bool(false)
bool(true)
