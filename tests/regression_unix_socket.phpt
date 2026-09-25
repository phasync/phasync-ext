--TEST--
Regression: unix:// sockets work with the extension loaded (ops-identity preserved for xport)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!defined('STREAM_PF_UNIX')) die('skip requires unix sockets');
if (!function_exists('stream_socket_server')) die('skip requires sockets');
?>
--FILE--
<?php
final class TestTimeout extends Exception {}
// Wrapping replaces stream->ops with a copy; the socket layer decides unix-vs-inet
// by ops-pointer identity, so a naive wrap made unix:// parse as ip:port ("Failed
// to parse address"). This must work both outside and inside a manage() scope.
$path = sys_get_temp_dir() . '/phasync_regr_' . getmypid() . '.sock';
@unlink($path);

function roundtrip(string $path): string {
    $srv = stream_socket_server("unix://$path", $errno, $err);
    if ($srv === false) return "server FAILED: $err";
    $cli = stream_socket_client("unix://$path", $errno, $err, 2);
    if ($cli === false) return "client FAILED: $err";
    $conn = stream_socket_accept($srv, 1);
    fwrite($cli, 'ping');
    usleep(20000);
    $got = fread($conn, 100);
    fclose($cli); fclose($conn); fclose($srv);
    return $got;
}

// Outside any scope.
echo "outside: " . roundtrip($path) . "\n";
@unlink($path);

// Inside a manage() scope.
echo "inside: " . \phasync\ext\manage(
    fn() => roundtrip($path),
    fn(...$a) => null, fn(...$a) => null, fn(...$a) => null, TestTimeout::class
) . "\n";
@unlink($path);
?>
--EXPECT--
outside: ping
inside: ping
