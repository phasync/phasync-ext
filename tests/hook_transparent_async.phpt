--TEST--
Transparent async: fread on a hooked socket suspends a fiber with its stream resource
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
        echo "fiber read: " . fread($conn, 100) . "\n";   // no data yet -> suspends
    });

    $res = $fiber->start();                 // handler was handed the client resource
    var_dump(is_resource($res));            // suspended on a real stream resource

    $sconn = stream_socket_accept($server, 1);
    fwrite($sconn, "hello");

    $r = [$res]; $w = $e = null;
    \phasync\ext\stream_select($r, $w, $e, 2);
    $fiber->resume();
    echo "done\n";
},
fn($res) => Fiber::suspend($res),
fn($res) => Fiber::suspend($res),
fn($us) => Fiber::suspend($us));
?>
--EXPECT--
bool(true)
fiber read: hello
done
