--TEST--
Transparent async: fread on a hooked socket suspends a fiber and resumes with data
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
\phasync\ext\manage(function () {
    $server = stream_socket_server("tcp://127.0.0.1:0", $errno, $errstr);
    $addr = stream_socket_get_name($server, false);

    $fiber = new Fiber(function() use ($addr) {
        $conn = stream_socket_client("tcp://$addr", $e, $es, 1);
        $data = fread($conn, 100);      // no data yet -> suspends transparently
        echo "fiber read: $data\n";
    });

    $sig = $fiber->start();             // runs until fread would block
    echo "suspended on: {$sig[0]}\n";

    $sconn = stream_socket_accept($server, 1);
    fwrite($sconn, "hello");

    [$type, $fd] = $sig;
    $r = [$fd]; $w = $e = null;
    \phasync\ext\stream_select($r, $w, $e, 2);
    $fiber->resume();
    echo "done\n";
},
fn($fd) => Fiber::suspend(['read', $fd]),
fn($fd) => Fiber::suspend(['write', $fd]),
fn($us) => Fiber::suspend(['sleep', $us]));
?>
--EXPECT--
suspended on: read
fiber read: hello
done
