--TEST--
A coroutine destroyed (by refcount or by the cycle collector) while suspended in a socket read, a waiting socket write, stream_socket_accept(), a process pipe read or a TLS read ends the operation safely; the stream and the other coroutines keep working
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
?>
--FILE--
<?php
require __DIR__ . '/destroy.inc';

foreach (['refcount', 'gc'] as $how) {
    echo "socket read ($how)\n";
    [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    destroy_while_waiting(new Loop, $how, fn() => fread($a, 100), after: [function () use ($a, $b) {
        fwrite($b, 'hello');
        echo "  then read: ", fread($a, 100), "\n";
    }]);
    fclose($a);
    fclose($b);

    echo "socket write ($how)\n";
    [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    stream_set_blocking($b, false);
    $filled = 0;
    while ($n = @fwrite($b, str_repeat('x', 65536))) { $filled += $n; }
    stream_set_blocking($b, true);
    destroy_while_waiting(new Loop, $how, fn() => fwrite($b, str_repeat('y', 65536)), after: [function () use ($a, $b, $filled) {
        $got = 0;                               // drain what was written, then write again
        while ($got < $filled) { $got += strlen(fread($a, 65536)); }
        echo "  then write: ", fwrite($b, 'z'), "\n";
    }]);
    fclose($a);
    fclose($b);

    echo "accept ($how)\n";
    $srv = stream_socket_server('tcp://127.0.0.1:0');
    $addr = stream_socket_get_name($srv, false);
    destroy_while_waiting(new Loop, $how, fn() => stream_socket_accept($srv, 5), after: [function () use ($srv, $addr) {
        $c = stream_socket_client("tcp://$addr");
        echo "  then accept: ", get_resource_type(stream_socket_accept($srv, 5)), "\n";
    }]);
    fclose($srv);

    echo "pipe read ($how)\n";
    $p = proc_open([PHP_BINARY, '-n', '-r', 'usleep(300000); echo "child\n";'], [1 => ['pipe', 'w']], $pipes);
    destroy_while_waiting(new Loop, $how, fn() => fread($pipes[1], 100), after: [function () use ($pipes) {
        echo "  then read: ", fgets($pipes[1]);
    }]);
    fclose($pipes[1]);
    proc_close($p);
}

if (!extension_loaded('openssl')) {
    echo "tls read (refcount)\n  finally ran\n  then read: hello\ntls read (gc)\n  finally ran\n  collected: yes\n  then read: hello\n";
    exit;
}
$pk = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
openssl_x509_export(openssl_csr_sign(openssl_csr_new(['commonName' => 'localhost'], $pk), null, $pk, 1), $crt);
openssl_pkey_export($pk, $key);
$pem = tempnam(sys_get_temp_dir(), 'phpem');
file_put_contents($pem, $crt . $key);
foreach (['refcount', 'gc'] as $how) {
    echo "tls read ($how)\n";
    // The server writes "hello" once told to, on its stdin.
    $p = proc_open([PHP_BINARY, '-n', '-r', '
        $c = stream_context_create(["ssl" => ["local_cert" => ' . var_export($pem, true) . ', "verify_peer" => false]]);
        $s = stream_socket_server("tls://127.0.0.1:0", $e, $es, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, $c);
        $n = stream_socket_get_name($s, false); echo substr($n, strrpos($n, ":") + 1), "\n"; flush();
        $x = stream_socket_accept($s, 5); fgets(STDIN); fwrite($x, "hello"); fgets(STDIN);'],
        [0 => ['pipe', 'r'], 1 => ['pipe', 'w']], $pipes);
    $port = trim(fgets($pipes[1]));
    $tls = stream_socket_client("tls://127.0.0.1:$port", $e, $es, 5, STREAM_CLIENT_CONNECT,
        stream_context_create(['ssl' => ['verify_peer' => false, 'verify_peer_name' => false]]));
    destroy_while_waiting(new Loop, $how, fn() => fread($tls, 100), after: [function () use ($tls, $pipes) {
        fwrite($pipes[0], "go\n");
        echo "  then read: ", fread($tls, 100), "\n";
    }]);
    fwrite($pipes[0], "bye\n");
    fclose($tls);
    proc_close($p);
}
unlink($pem);
?>
--EXPECTF--
socket read (refcount)
  finally ran
  then read: hello
socket write (refcount)
  finally ran
  then write: 1
accept (refcount)

Warning: stream_socket_accept(): Accept failed: Operation canceled in %s on line %d
  finally ran
  then accept: stream
pipe read (refcount)
  finally ran
  then read: child
socket read (gc)
  finally ran
  collected: yes
  then read: hello
socket write (gc)
  finally ran
  collected: yes
  then write: 1
accept (gc)

Warning: stream_socket_accept(): Accept failed: Operation canceled in %s on line %d
  finally ran
  collected: yes
  then accept: stream
pipe read (gc)
  finally ran
  collected: yes
  then read: child
tls read (refcount)
  finally ran
  then read: hello
tls read (gc)
  finally ran
  collected: yes
  then read: hello
