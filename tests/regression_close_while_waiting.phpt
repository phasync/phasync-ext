--TEST--
Regression: a stream closed by one coroutine while another is suspended in an operation on it fails that operation (EBADF) instead of freeing the stream under it (#14)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
?>
--FILE--
<?php
// The closer waits in its close until the suspended coroutine has left the
// operation; the operation fails as on a closed descriptor. Up to 0.5.0-alpha13
// the stream was freed under the suspended coroutine (use-after-free, segfault).
require __DIR__ . '/loop.inc';

function race(string $name, Closure $op, Closure $close): void {
    $log = [];
    (new Loop)->runAll(
        function () use ($op, &$log) { $log[] = 'op: ' . json_encode($op()); },
        function () use ($close, &$log) { usleep(20000); $close(); $log[] = 'closed'; },
    );
    printf("%-12s %s\n", $name, implode(', ', $log));
}

[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
race('fread', fn() => fread($a, 100), fn() => fclose($a));
fclose($b);

[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
stream_set_blocking($b, false);
while (@fwrite($b, str_repeat('x', 65536))) {}      // fill the buffer: fwrite() must wait
stream_set_blocking($b, true);
race('fwrite', fn() => fwrite($b, str_repeat('x', 65536)), fn() => fclose($b));
fclose($a);

$srv = stream_socket_server('tcp://127.0.0.1:0');
race('accept', function () use ($srv) {
    $r = @stream_socket_accept($srv, 5);
    return [$r, error_get_last()['message'] ?? null];
}, fn() => fclose($srv));

$p = proc_open([PHP_BINARY, '-n', '-r', 'sleep(2);'], [1 => ['pipe', 'w']], $pipes);
race('pipe fread', fn() => fread($pipes[1], 100), fn() => fclose($pipes[1]));
proc_terminate($p);
proc_close($p);

if (extension_loaded('openssl')) {
    $pk  = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
    openssl_x509_export(openssl_csr_sign(openssl_csr_new(['commonName' => 'localhost'], $pk), null, $pk, 1), $crt);
    openssl_pkey_export($pk, $key);
    $pem = tempnam(sys_get_temp_dir(), 'phpem');
    file_put_contents($pem, $crt . $key);
    $p = proc_open([PHP_BINARY, '-n', '-r', '
        $c = stream_context_create(["ssl" => ["local_cert" => ' . var_export($pem, true) . ', "verify_peer" => false]]);
        $s = stream_socket_server("tls://127.0.0.1:0", $e, $es, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, $c);
        $n = stream_socket_get_name($s, false); echo substr($n, strrpos($n, ":") + 1), "\n"; flush();
        $x = @stream_socket_accept($s, 5); sleep(2);'], [1 => ['pipe', 'w']], $pipes);
    $port = trim(fgets($pipes[1]));
    $tls = stream_socket_client("tls://127.0.0.1:$port", $e, $es, 5, STREAM_CLIENT_CONNECT,
        stream_context_create(['ssl' => ['verify_peer' => false, 'verify_peer_name' => false]]));
    race('tls fread', fn() => fread($tls, 100), fn() => fclose($tls));
    proc_terminate($p);
    proc_close($p);
    unlink($pem);
} else {
    echo "tls fread    op: false, closed\n";
}

$file = tempnam(sys_get_temp_dir(), 'phasync_close_');
$holder = fopen($file, 'r+');
flock($holder, LOCK_EX);
$fp = fopen($file, 'r+');
race('flock', fn() => flock($fp, LOCK_EX), fn() => fclose($fp));
fclose($holder);

// On the thread pool the operation can't be interrupted: the close waits for it.
ini_set('phasync.fs_offload', 'all');
file_put_contents($file, str_repeat('y', 1000));
$fp = fopen($file, 'r');
race('pool fread', fn() => strlen(fread($fp, 100)), fn() => fclose($fp));
$fp = fopen($file, 'w');
race('pool fwrite', fn() => fwrite($fp, str_repeat('z', 100000)), fn() => fclose($fp));
ini_restore('phasync.fs_offload');
unlink($file);

// A cancellation of the closer's wait is held until the reader has left.
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
$loop = new Loop;
$closer = new Fiber(function () use ($a) {
    usleep(20000);
    try { fclose($a); echo "closer: closed\n"; } catch (LoopCancelled $e) { echo "closer: ", $e->getMessage(), ", ", get_resource_type($a), "\n"; }
});
$loop->onPark = function () use ($loop, $closer) {
    if (Fiber::getCurrent() === $closer) {       // the closer parks in fclose(): cancel it at once
        $loop->onPark = null;
        $loop->go(fn() => $loop->cancel($closer));
    }
};
$loop->runAll(function () use ($a) { echo "reader: ", json_encode(fread($a, 100)), "\n"; }, $closer);
fclose($b);
?>
--EXPECT--
fread        op: false, closed
fwrite       op: false, closed
accept       op: [false,"stream_socket_accept(): Accept failed: Bad file descriptor"], closed
pipe fread   op: false, closed
tls fread    op: false, closed
flock        op: false, closed
pool fread   op: 100, closed
pool fwrite  op: 100000, closed
reader: false
closer: cancelled, Unknown
