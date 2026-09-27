--TEST--
stream_select() inside a scope parks the coroutine (others keep running) and keeps native results/timeouts
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

// A's stream_select() parks it; B runs meanwhile and makes A's stream readable.
$log = [];
(new Loop)->runAll(
    function () use ($a, &$log) {
        $r = ['key' => $a]; $w = $e = null;
        $n = stream_select($r, $w, $e, 5);        // nothing ready yet -> parks
        $log[] = "A: $n " . implode(',', array_keys($r));
    },
    function () use ($b, &$log) {
        usleep(100000);
        $log[] = 'B: writes';
        fwrite($b, 'x');                          // wakes A
    },
);
echo implode("\n", $log), "\n";

// Timeout: the loop times the wait out, and select returns 0 with every array emptied.
fread($a, 1);
(new Loop)->runAll(function () use ($a) {
    $r = [$a]; $w = []; $e = [$a];
    $t = microtime(true);
    var_dump(stream_select($r, $w, $e, 0, 200000), $r, $w, $e);
    $el = microtime(true) - $t;
    var_dump($el > 0.15 && $el < 1.0);
});
?>
--EXPECT--
B: writes
A: 1 key
int(0)
array(0) {
}
array(0) {
}
array(0) {
}
bool(true)
