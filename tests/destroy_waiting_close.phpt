--TEST--
A coroutine destroyed while another closes the stream it waits on lets the close go on; a closer destroyed while it waits for the others leaves the stream closing, freed with its last reference (#14)
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/destroy.inc';

foreach (['refcount', 'gc'] as $how) {
    // The closer waits in fclose() for the reader to leave its read; the reader is
    // destroyed instead: its read ends, and the close goes on.
    echo "reader destroyed while another closes ($how)\n";
    [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    destroy_while_waiting(new Loop, $how, fn() => fread($a, 100), alongside: [function () use ($a) {
        $r = fclose($a);
        echo "  closer: ", json_encode($r), ", ", get_resource_type($a), "\n";
    }]);
    fclose($b);

    // The closer is destroyed while it waits: it cannot, so it leaves the stream
    // closing (the reader's read fails) and open until its last reference goes.
    echo "closer destroyed while another reads ($how)\n";
    [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    destroy_while_waiting(new Loop, $how, fn() => fclose($a), before: [function () use ($a) {
        $r = fread($a, 100);
        echo "  reader: ", json_encode($r), "\n";
    }]);
    echo "  stream: ", get_resource_type($a), "\n";
    fwrite($b, 'x');
    echo "  read without waiting: ", fread($a, 100), "\n";
    fclose($a);
    echo "  closed: ", get_resource_type($a), "\n";
    fclose($b);

    // Two readers, a closer and a second fclose() while the first waits.
    echo "one of two readers destroyed, a second fclose() ($how)\n";
    [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    [$c, $d] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    destroy_while_waiting(new Loop, $how, fn() => fread($a, 100), before: [function () use ($c) {
        $r = fread($c, 100);
        echo "  other reader: ", json_encode($r), "\n";
    }], alongside: [function () use ($a) {
        $r = fclose($a);
        echo "  closer: ", json_encode($r), ", ", get_resource_type($a), "\n";
    }, function () use ($a, $d) {
        $r = fclose($a);
        echo "  second closer: ", json_encode($r), "\n";
        fwrite($d, 'y');
    }]);
    fclose($b);
    fclose($c);
    fclose($d);
}
?>
--EXPECT--
reader destroyed while another closes (refcount)
  finally ran
  closer: true, Unknown
closer destroyed while another reads (refcount)
  finally ran
  reader: false
  stream: stream
  read without waiting: x
  closed: Unknown
one of two readers destroyed, a second fclose() (refcount)
  second closer: true
  finally ran
  closer: true, Unknown
  other reader: "y"
reader destroyed while another closes (gc)
  finally ran
  collected: yes
  closer: true, Unknown
closer destroyed while another reads (gc)
  finally ran
  collected: yes
  reader: false
  stream: stream
  read without waiting: x
  closed: Unknown
one of two readers destroyed, a second fclose() (gc)
  second closer: true
  finally ran
  collected: yes
  closer: true, Unknown
  other reader: "y"
