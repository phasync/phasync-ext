--TEST--
socket_select() waiting on a socket another coroutine closes returns at once, reporting it
ready in the sets it was waiting in, not at its timeout (#16)
--EXTENSIONS--
phasync
sockets
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$pair = [];
socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $pair);
[$a, $b] = $pair;

$log = [];
(new Loop)->runAll(
    function () use ($a, &$log) {
        $r = [$a]; $w = null; $e = null;
        $t = microtime(true);
        $n = @socket_select($r, $w, $e, 5);
        $el = microtime(true) - $t;
        $log[] = "A: $n " . count($r) . ' fast=' . ($el < 1.0 ? 'yes' : 'no');
    },
    function () use ($a, &$log) {
        usleep(100000);
        $log[] = 'B: closes';
        socket_close($a);
    },
);
echo implode("\n", $log), "\n";
socket_close($b);
?>
--EXPECT--
B: closes
A: 1 1 fast=yes
