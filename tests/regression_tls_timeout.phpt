--TEST--
Regression: tls:// streams pass their stream_set_timeout() to park() and time out like native
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!extension_loaded('openssl')) die('skip requires openssl');
if (!function_exists('proc_open')) die('skip requires proc_open');
?>
--FILE--
<?php
// TLS streams use ext/openssl's socket ops; alpha7 didn't recognise them as
// sockets and handed the handler null. Native: fgets() returns the partial line,
// fread() then returns false, timed_out set both times.
require __DIR__ . '/loop.inc';

$pk  = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
$csr = openssl_csr_new(['commonName' => 'localhost'], $pk);
openssl_x509_export(openssl_csr_sign($csr, null, $pk, 1), $crt);
openssl_pkey_export($pk, $key);
$pem = tempnam(sys_get_temp_dir(), 'phpem');
file_put_contents($pem, $crt . $key);

$server = '
$c = stream_context_create(["ssl" => ["local_cert" => ' . var_export($pem, true) . ', "verify_peer" => false]]);
$s = stream_socket_server("tls://127.0.0.1:0", $e, $es, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, $c);
$n = stream_socket_get_name($s, false); echo substr($n, strrpos($n, ":") + 1), "\n"; flush();
$x = @stream_socket_accept($s, 5);
if ($x) { fwrite($x, "partial"); sleep(2); }';
$p = proc_open([PHP_BINARY, '-n', '-r', $server], [1 => ['pipe', 'w']], $pipes);
$port = trim(fgets($pipes[1]));

$ctx = stream_context_create(['ssl' => ['verify_peer' => false, 'verify_peer_name' => false]]);
$cli = stream_socket_client("tls://127.0.0.1:$port", $e, $es, 5, STREAM_CLIENT_CONNECT, $ctx);
stream_set_timeout($cli, 0, 200000);

$loop = new Loop;
$loop->runAll(function () use ($cli) {
    var_dump(fgets($cli));                               // string(7) "partial"
    var_dump(stream_get_meta_data($cli)['timed_out']);   // bool(true)
    var_dump(fread($cli, 10));                           // bool(false)
    var_dump(stream_get_meta_data($cli)['timed_out']);   // bool(true)
});
$seen = $loop->parkTimeouts[0] ?? null;

var_dump(is_float($seen) && $seen > 0 && $seen <= 0.2);   // bool(true): the socket's timeout, not null
proc_terminate($p);
foreach ($pipes as $pp) fclose($pp);
proc_close($p);
@unlink($pem);
?>
--EXPECT--
string(7) "partial"
bool(true)
bool(false)
bool(true)
bool(true)
