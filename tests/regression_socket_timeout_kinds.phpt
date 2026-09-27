--TEST--
Regression: every socket kind passes its stream_set_timeout() to park() (tcp client/accepted, unix, udp, socketpair)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!defined('STREAM_PF_UNIX')) die('skip requires unix sockets');
?>
--FILE--
<?php
// alpha7 handed tcp sockets a null timeout: ext/openssl registers itself for
// tcp://, so TCP streams don't use xp_socket's ops and weren't recognised as
// sockets. The loop times the wait out after the timeout park() is given, so
// each case must match native: partial line, timed_out.
require __DIR__ . '/loop.inc';

function probe(string $label, $reader, $writer): void {
    stream_set_timeout($reader, 0, 200000);
    fwrite($writer, 'partial');
    $loop = new Loop;
    $loop->runAll(function () use ($label, $reader, $loop) {
        $line = fgets($reader);
        $seen = $loop->parkTimeouts[0] ?? null;
        printf("%s: timeout=%s line=%s timed_out=%s\n", $label,
            $seen !== null && abs($seen - 0.2) < 0.01 ? '0.2' : var_export($seen, true),
            var_export($line, true), var_export(stream_get_meta_data($reader)['timed_out'], true));
    });
}

$srv = stream_socket_server('tcp://127.0.0.1:0');
$cli = stream_socket_client('tcp://' . stream_socket_get_name($srv, false));
$acc = stream_socket_accept($srv);
probe('tcp client', $cli, $acc);
probe('tcp accepted', $acc, $cli);

$path = sys_get_temp_dir() . '/phasync_to_' . getmypid() . '.sock';
@unlink($path);
$usrv = stream_socket_server("unix://$path");
$ucli = stream_socket_client("unix://$path");
$uacc = stream_socket_accept($usrv);
probe('unix', $ucli, $uacc);

$dsrv = stream_socket_server('udp://127.0.0.1:0', $errno, $errstr, STREAM_SERVER_BIND);
$dcli = stream_socket_client('udp://' . stream_socket_get_name($dsrv, false));
probe('udp', $dsrv, $dcli);

[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
probe('socketpair', $a, $b);
@unlink($path);
?>
--EXPECT--
tcp client: timeout=0.2 line='partial' timed_out=true
tcp accepted: timeout=0.2 line='partial' timed_out=true
unix: timeout=0.2 line='partial' timed_out=true
udp: timeout=0.2 line='partial' timed_out=true
socketpair: timeout=0.2 line='partial' timed_out=true
