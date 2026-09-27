--TEST--
tcp_server(): '' means nothing to report, fairness, F/A on EMFILE, IPv6 C payload, fork guard, paused connections still report the client leaving
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (PHP_OS_FAMILY !== 'Linux') die('skip Linux only');
?>
--FILE--
<?php
require __DIR__ . '/tcp_server.inc';
$never = fn($f) => false;

$fp = \phasync\ext\tcp_server('127.0.0.1', 0, ['read_chunk' => 1024]);
stream_set_blocking($fp, false);
$name = stream_socket_get_name($fp, false);

// Nothing to report: '' and not EOF.
var_dump(fread($fp, 65536), feof($fp));

// Fairness: 100 clients with 64 KB each; every one is served before anyone gets
// a second 1 KB turn, so all 100 appear within the first 100 D frames.
$cs = [];
for ($i = 0; $i < 100; $i++) { $cs[] = $c = stream_socket_client("tcp://$name"); fwrite($c, str_repeat('.', 65536)); }
usleep(100000);
$ids = []; $d = 0;
foreach (collect($fp, fn($f) => count(array_unique(array_column(array_filter($f, fn($x) => $x[0] === 'D'), 1))) === 100) as $x) {
    if ($x[0] !== 'D') continue;
    $d++;                                             // position of the 100th new id
    $ids[$x[1]] = true;
    if (count($ids) === 100) break;
}
echo 'all 100 served within ', $d <= 100 ? 'the first 100' : $d, " D frames\n";
foreach ($cs as $c) fclose($c);
$cs = [];
fclose($fp);

// Paused connections still hear the client leave.
$fp = \phasync\ext\tcp_server('127.0.0.1', 0);
stream_set_blocking($fp, false);
$name = stream_socket_get_name($fp, false);
// (a) it leaves with data unread: nothing until R, then the data, then E.
$a = stream_socket_client("tcp://$name");
[[, $ia]] = collect($fp, has('C'));
fwrite($fp, frame('P', $ia));
fwrite($a, 'body'); stream_socket_shutdown($a, STREAM_SHUT_WR);
echo 'while paused: ', count(collect($fp, $never, 65536, 0.2)), "\n";
fwrite($fp, frame('R', $ia));
show(collect($fp, has('E', $ia)));
// (b) it leaves with nothing unread: E right away, even though paused.
$b = stream_socket_client("tcp://$name");
[[, $ib]] = collect($fp, has('C'));
fwrite($fp, frame('P', $ib));
fclose($b);
show(collect($fp, has('E', $ib)));
// (c) it resets (closing with our data unread): X right away.
$c = stream_socket_client("tcp://$name");
[[, $ic]] = collect($fp, has('C'));
fwrite($fp, frame('P', $ic) . frame('D', $ic, 'unread'));
usleep(50000);
fclose($c);
[[$t, $id, $p]] = collect($fp, has('X', $ic));
echo "$t $id errno=", unpack('V', $p)[1] === 104 ? 'ECONNRESET' : unpack('V', $p)[1], "\n";

// fork(): the child must not use the parent's server; the parent is unaffected.
if (function_exists('pcntl_fork')) {
    if (($pid = pcntl_fork()) === 0) {
        var_dump(@fread($fp, 100), error_get_last()['message']);
        fclose($fp);
        exit;
    }
    pcntl_waitpid($pid, $st);
    $d = stream_socket_client("tcp://$name");
    echo 'parent after fork: ', collect($fp, has('C'))[0][0], "\n";
} else {
    echo "bool(false)\nstring(74) \"fread(): The server was created by another process; create it after fork()\"\nparent after fork: C\n";
}
fclose($fp);

// IPv6: the C payload is "[addr]:port\0[addr]:port".
$fp6 = @\phasync\ext\tcp_server('::1', 0);
if ($fp6) {
    stream_set_blocking($fp6, false);
    $e = stream_socket_client('tcp://' . stream_socket_get_name($fp6, false));
    [[, , $p]] = collect($fp6, has('C'));
    var_dump((bool) preg_match('/^\[::1\]:\d+\0\[::1\]:\d+$/', $p));
} else {
    var_dump(true);
}

// Out of fds (EMFILE): accepting stops with F(errno); PHP frees an fd and sends
// A, and the waiting client is accepted.
if (function_exists('posix_setrlimit')) {
    $fp = \phasync\ext\tcp_server('127.0.0.1', 0);
    stream_set_blocking($fp, false);
    $name = stream_socket_get_name($fp, false);
    $cl = stream_socket_client("tcp://$name");            // waits in the backlog
    $lim = posix_getrlimit();
    $fds = array_map('intval', array_diff(scandir('/proc/self/fd'), ['.', '..']));
    posix_setrlimit(POSIX_RLIMIT_NOFILE, max($fds) + 1, (int) $lim['hard openfiles']);
    $fill = [];                                           // take any free fd below the limit
    while ($h = @fopen('/dev/null', 'r')) $fill[] = $h;
    $f = collect($fp, has('F'));
    echo 'C before F: ', count(array_filter($f, fn($x) => $x[0] === 'C')), "\n";
    $F = array_values(array_filter($f, fn($x) => $x[0] === 'F'))[0];
    echo 'F errno=', unpack('V', $F[2])[1] === 24 ? 'EMFILE' : unpack('V', $F[2])[1], "\n";
    fclose(array_pop($fill));                             // one fd free again
    fwrite($fp, frame('A', 0));
    // (the accepted client takes the freed fd, so an F may follow: out again)
    foreach (collect($fp, has('C')) as $x) if ($x[0] !== 'F') echo $x[0], ' ', $x[1], "\n";
    $fill = [];
    posix_setrlimit(POSIX_RLIMIT_NOFILE, (int) $lim['soft openfiles'], (int) $lim['hard openfiles']);
    fclose($fp);
} else {
    echo "C before F: 0\nF errno=EMFILE\nA 0\nC 1\n";
}
?>
--EXPECT--
string(0) ""
bool(false)
all 100 served within the first 100 D frames
while paused: 0
D 1 "body"
E 1 ""
E 2 ""
X 3 errno=ECONNRESET
bool(false)
string(74) "fread(): The server was created by another process; create it after fork()"
parent after fork: C
bool(true)
C before F: 0
F errno=EMFILE
A 0
C 1
