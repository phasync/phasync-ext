--TEST--
stream_select() waiting on a stream another coroutine closes returns at once, reporting it
ready in the sets it was waiting in, not at its timeout (#16)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);

// A parks in stream_select() on $a (in both its read and except sets); B closes $a shortly
// after. A must wake at once (well under the 5s timeout), with $a reported ready in both
// sets, at its original keys -- not with the native timeout's empty arrays.
$log = [];
(new Loop)->runAll(
    function () use ($a, &$log) {
        $r = ['rk' => $a]; $w = null; $e = ['ek' => $a];
        $t = microtime(true);
        $n = stream_select($r, $w, $e, 5);
        $el = microtime(true) - $t;
        $log[] = "A: $n r=" . implode(',', array_keys($r)) . ' e=' . implode(',', array_keys($e))
            . ' fast=' . ($el < 1.0 ? 'yes' : 'no');
    },
    function () use ($a, &$log) {
        usleep(100000);
        $log[] = 'B: closes';
        fclose($a);
    },
);
echo implode("\n", $log), "\n";

// The caller's next op on it fails as it would natively on a closed stream (a TypeError, or a
// warning + false, depending on the PHP version).
[$c, $d] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
(new Loop)->runAll(
    function () use ($c) {
        $r = [$c]; $w = $e = null;
        stream_select($r, $w, $e, 5);
        try {
            var_dump(@fread($r[0], 1));
        } catch (\TypeError $e) {
            echo "TypeError\n";
        }
    },
    function () use ($c) {
        usleep(100000);
        fclose($c);
    },
);
fclose($d);
fclose($b);
?>
--EXPECTF--
B: closes
A: 2 r=rk e=ek fast=yes
%s
