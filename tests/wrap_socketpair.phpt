--TEST--
stream_socket_pair() streams are wrapped and cooperate (fread suspends, resumes with data)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair');
?>
--FILE--
<?php
final class TestTimeout extends Exception {}
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);

\phasync\ext\manage(function () use ($a, $b) {
    // Functional proof: a blocking fread on the empty pair suspends the fiber,
    // then resumes with the data once the other end is written.
    $reader = new Fiber(function () use ($a) {
        echo "read: " . fread($a, 100) . "\n";   // no data yet -> suspends
    });
    $sig = $reader->start();
    var_dump($sig === $a);                 // bool(true): suspended waiting on $a itself
    fwrite($b, 'ping');                     // make $a readable
    $r = [$sig]; $w = $e = null;
    \phasync\ext\stream_select($r, $w, $e, 2);
    $reader->resume();
}, fn($res) => Fiber::suspend($res), fn($res) => Fiber::suspend($res), fn($us) => Fiber::suspend($us), TestTimeout::class);

fclose($a); fclose($b);
?>
--EXPECT--
bool(true)
read: ping
