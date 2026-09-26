--TEST--
stream_select() inside a scope suspends the fiber (others keep running) and keeps native results/timeouts
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair');
?>
--FILE--
<?php
final class TestTimeout extends Exception {}

// Minimal scheduler: handlers suspend with what they wait on; the loop waits with
// phasync\ext\stream_select() (not overridden) and resumes whoever is ready.
function run(array $fibers): void {
    $wait = [];
    foreach ($fibers as $k => $f) $wait[$k] = $f->start();
    while ($wait = array_filter($wait)) {
        $r = $w = []; $e = null; $to = 1.0; $now = microtime(true);
        foreach ($wait as $k => [$type, $x]) {
            if ($type === 'r') $r[$k] = $x; elseif ($type === 'w') $w[$k] = $x; else $to = min($to, max(0, $x - $now));
        }
        if ($r || $w) \phasync\ext\stream_select($r, $w, $e, 0, (int) ($to * 1e6)); else usleep((int) ($to * 1e6));
        $now = microtime(true);
        foreach ($wait as $k => [$type, $x]) {
            if (($type === 'r' && isset($r[$k])) || ($type === 'w' && isset($w[$k])) || ($type === 't' && $x <= $now)) {
                $wait[$k] = $fibers[$k]->resume();
            }
        }
    }
}
$h = [
    fn($s, $t) => Fiber::suspend(['r', $s]),
    fn($s, $t) => Fiber::suspend(['w', $s]),
    fn($us) => Fiber::suspend(['t', microtime(true) + $us / 1e6]),
];

[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
$log = [];
\phasync\ext\manage(function () use ($a, $b, &$log) {
    run([
        'A' => new Fiber(function () use ($a, &$log) {
            $r = ['key' => $a]; $w = $e = null;
            $n = stream_select($r, $w, $e, 5);        // nothing ready yet -> suspends
            $log[] = "A: $n " . implode(',', array_keys($r));
        }),
        'B' => new Fiber(function () use ($b, &$log) {
            usleep(100000);
            $log[] = 'B: writes';
            fwrite($b, 'x');                          // wakes A
        }),
    ]);
}, ...[...$h, TestTimeout::class]);
echo implode("\n", $log), "\n";

// Timeout: a handler that really waits (like phasync) and then signals timeout.
$waitReal = function ($s, $t) {
    $r = [$s]; $w = $e = null;
    if (\phasync\ext\stream_select($r, $w, $e, 0, (int) ($t * 1e6)) < 1) throw new TestTimeout();
};
fread($a, 1);
(new Fiber(fn() => \phasync\ext\manage(function () use ($a) {
    $r = [$a]; $w = []; $e = [$a];
    $t = microtime(true);
    var_dump(stream_select($r, $w, $e, 0, 200000), $r, $w, $e);
    $el = microtime(true) - $t;
    var_dump($el > 0.15 && $el < 1.0);
}, $waitReal, $waitReal, fn($us) => null, TestTimeout::class)))->start();
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
