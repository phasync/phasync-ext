--TEST--
tcp_server(): many clients in whole-frame reads, max_connections, read_chunk, backpressure (W), pause/resume, partial reads, blocking read in a fiber
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (function_exists('posix_getrlimit') && posix_getrlimit()['soft openfiles'] < 1024) die('skip needs 1024 fds');
?>
--FILE--
<?php
require __DIR__ . '/tcp_server.inc';
$never = fn($f) => false;

// 300 clients connect and send: every read parses as whole frames (parse()
// throws otherwise), and each id gets its C before its D.
$fp = \phasync\ext\tcp_server('127.0.0.1', 0);
stream_set_blocking($fp, false);
$name = stream_socket_get_name($fp, false);
$clients = [];
for ($i = 0; $i < 300; $i++) {
    $clients[$i] = stream_socket_client("tcp://$name");
    fwrite($clients[$i], "hi $i");
}
$frames = collect($fp, fn($f) => count(array_filter($f, fn($x) => $x[0] === 'D')) === 300);
$seen = []; $ok = true;
foreach ($frames as [$t, $id, $p]) {
    if ($t === 'C') $seen[$id] = true;
    if ($t === 'D') $ok = $ok && isset($seen[$id]) && $p === 'hi ' . ($id - 1);
}
echo 'clients: ', count($seen), ' ', var_export($ok, true), "\n";
fclose($fp); $clients = [];

$fp = \phasync\ext\tcp_server('127.0.0.1', 0, ['max_connections' => 2, 'read_chunk' => 4096, 'high_water' => 65536, 'nodelay' => true]);
stream_set_blocking($fp, false);
$name = stream_socket_get_name($fp, false);

// max_connections: accepting stops (F, reason 0) and the third client waits in
// the backlog until one closes (A, then its C, and F again: full once more).
$a = stream_socket_client("tcp://$name"); $b = stream_socket_client("tcp://$name"); $c = stream_socket_client("tcp://$name");
$f = collect($fp, $never, 65536, 0.3);
echo 'accepted: ', count(array_filter($f, fn($x) => $x[0] === 'C')), "\n";
show(array_values(array_filter($f, fn($x) => $x[0] === 'F')));
fwrite($fp, frame('X', 2));
show(collect($fp, has('C', 3)));
fclose($b);

// read_chunk: 100 KB from a client arrives as D frames of at most 4096 bytes.
fwrite($a, str_repeat('x', 100000));
$d = collect($fp, fn($f) => array_sum(array_map(fn($x) => strlen($x[2]), $f)) >= 100000);
echo 'chunks: ', count($d), ' max ', max(array_map(fn($x) => strlen($x[2]), $d)), ' total ', array_sum(array_map(fn($x) => strlen($x[2]), $d)), "\n";

// Backpressure: 16 MB to a client that isn't reading overflows the kernel's
// socket buffers and goes past high_water: B at once, W once the client has read it.
var_dump(fwrite($fp, frame('D', 1, str_repeat('y', 16 << 20))));
$early = collect($fp, has('B', 1));
echo 'B: ', var_export((has('B', 1))($early), true), ', W early: ', var_export((has('W', 1))($early), true), "\n";
fwrite($fp, frame('D', 1, str_repeat('y', 1 << 20)));   // still over: no second B
$got = 0; $w = [];
stream_set_blocking($a, false);
for ($t = microtime(true); $got < (17 << 20) && microtime(true) - $t < 20; ) {
    $n = strlen(fread($a, 1 << 20));
    $chunk = fread($fp, 65536);                        // non-blocking: also flushes
    if ($chunk !== '') array_push($w, ...parse($chunk));
    if (!$n && $chunk === '') usleep(1000);
    $got += $n;
}
stream_set_blocking($a, true);
$w = array_merge($early, $w, collect($fp, has('W', 1)));
echo 'W after drain: ', var_export((has('W', 1))($w), true), ', B frames: ', count(array_filter($w, fn($x) => $x[0] === 'B')), "\n";

// Pause/resume: no D while paused; it arrives after R.
fwrite($fp, frame('P', 1));
fwrite($a, 'during pause');
echo 'D while paused: ', count(collect($fp, $never, 65536, 0.2)), "\n";
fwrite($fp, frame('R', 1));
show(collect($fp, has('D', 1)));

// A frame bigger than the read length is handed out in parts.
fwrite($a, str_repeat('z', 3000));
usleep(50000);
$buf = '';
while (strlen($buf) < 13 + 3000) { $r = [$fp]; $wr = $e = null; stream_select($r, $wr, $e, 1); $buf .= fread($fp, 1000); }
[[$t, $id, $p]] = parse($buf);
echo "parts: $t $id ", strlen($p), "\n";

// A blocking read inside a fiber waits through the read handler.
stream_set_blocking($fp, true);
$waited = false;
$handler = function ($s, $t) use (&$waited, $fp, $a) {
    $waited = ($s === $fp);
    fwrite($a, 'wake');
    $r = [$s]; $w = $e = null; stream_select($r, $w, $e, 5);
};
$code = function () use ($fp, &$res) { $res = parse(fread($fp, 65536)); };
(new Fiber(fn() => \phasync\ext\manage($code, $handler, $handler, fn($us) => null, Exception::class)))->start();
var_dump($waited); show($res);
fclose($fp);
?>
--EXPECTF--
clients: 300 true
accepted: 2
F 0 "\u0000\u0000\u0000\u0000"
X 2 "\u0000\u0000\u0000\u0000"
A 0 ""
C 3 "127.0.0.1:%d\u0000127.0.0.1:%d"
F 0 "\u0000\u0000\u0000\u0000"
chunks: 25 max 4096 total 100000
int(16777229)
B: true, W early: false
W after drain: true, B frames: 1
D while paused: 0
D 1 "during pause"
parts: D 1 3000
bool(true)
D 1 "wake"
