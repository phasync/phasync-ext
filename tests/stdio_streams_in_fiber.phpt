--TEST--
STDIN reads and STDOUT/STDERR writes on pipes suspend the fiber inside a scope, with native results
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
?>
--FILE--
<?php
// A child PHP does one stdio op in a fiber beside a ticker; the parent withholds
// stdin (or leaves stdout/stderr unread) for 300ms. The child reports on fd 3.
$child = tempnam(sys_get_temp_dir(), 'phasync_stdio_') . '.php';
file_put_contents($child, <<<'PHP'
<?php
final class T extends Exception {}
function run(array $fibers): void {
    $wait = [];
    foreach ($fibers as $k => $f) $wait[$k] = $f->start();
    while ($wait = array_filter($wait)) {
        $r = $w = []; $e = null; $to = 1.0; $now = microtime(true);
        foreach ($wait as $k => [$type, $x]) { if ($type === 'r') $r[$k] = $x; elseif ($type === 'w') $w[$k] = $x; else $to = min($to, max(0, $x - $now)); }
        if ($r || $w) \phasync\ext\stream_select($r, $w, $e, 0, (int) ($to * 1e6)); else usleep((int) ($to * 1e6));
        $now = microtime(true);
        foreach ($wait as $k => [$type, $x]) if (($type === 'r' && isset($r[$k])) || ($type === 'w' && isset($w[$k])) || ($type === 't' && $x <= $now)) $wait[$k] = $fibers[$k]->resume();
    }
}
$mode = $argv[1]; $ticks = 0; $done = false; $res = null;
\phasync\ext\manage(function () use ($mode, &$ticks, &$done, &$res) {
    run([
        'W' => new Fiber(function () use ($mode, &$done, &$res) {
            $res = match ($mode) {
                'fgets(STDIN)'        => fgets(STDIN),
                'fread(php://stdin)'  => fread(fopen('php://stdin', 'r'), 100),
                'fwrite(STDOUT)'      => fwrite(STDOUT, str_repeat('x', 1 << 20)),
                'fwrite(STDERR)'      => fwrite(STDERR, str_repeat('z', 1 << 20)),
            };
            $done = true;
        }),
        'T' => new Fiber(function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } }),
    ]);
}, fn($s, $t) => Fiber::suspend(['r', $s]), fn($s, $t) => Fiber::suspend(['w', $s]),
   fn($us) => Fiber::suspend(['t', microtime(true) + $us / 1e6]), T::class);
fprintf(fopen('php://fd/3', 'w'), "%-20s %s %s\n", $mode, json_encode($res), $ticks >= 5 ? 'cooperative' : "BLOCKED (ticks=$ticks)");
PHP);
foreach (['fgets(STDIN)', 'fread(php://stdin)', 'fwrite(STDOUT)', 'fwrite(STDERR)'] as $mode) {
    $cmd = [PHP_BINARY, '-n', '-d', 'extension_dir=' . ini_get('extension_dir'), '-d', 'extension=phasync', $child, $mode];
    $p = proc_open($cmd, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w'], 3 => ['pipe', 'w']], $pipes);
    usleep(300000);
    fwrite($pipes[0], "hello\n"); fclose($pipes[0]);
    $len = [1 => 0, 2 => 0]; $open = [1 => $pipes[1], 2 => $pipes[2]];
    while ($open) {                                  // drain stdout and stderr together
        $r = $open; $w = $e = null; stream_select($r, $w, $e, 10);
        foreach ($r as $k => $s) { $d = fread($s, 65536); $len[$k] += strlen($d); if ($d === '' && feof($s)) unset($open[$k]); }
    }
    echo trim(stream_get_contents($pipes[3])), " out=$len[1] err=$len[2]\n";
    proc_close($p);
}
unlink($child);
?>
--EXPECT--
fgets(STDIN)         "hello\n" cooperative out=0 err=0
fread(php://stdin)   "hello\n" cooperative out=0 err=0
fwrite(STDOUT)       1048576 cooperative out=1048576 err=0
fwrite(STDERR)       1048576 cooperative out=0 err=1048576
