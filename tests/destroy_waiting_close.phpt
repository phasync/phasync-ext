--TEST--
A coroutine destroyed while another closes the stream it waits on lets the close go on; a closer destroyed while it waits is held until the others have left, then closes; fclose() by a coroutine being destroyed leaves the stream to its last reference (#14, #17, #21)
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

    // The closer, dropped while it waits, can't be destroyed there: the extension
    // holds it until the reader has left (its read fails), and lets PHP have it
    // where no coroutine is inside an op (here: the next manage()). Its close
    // then ends as it unwinds. (gc: the cycle collector didn't take it while held.)
    echo "closer destroyed while another reads ($how)\n";
    [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    destroy_while_waiting(new Loop, $how, fn() => fclose($a), before: [function () use ($a) {
        $r = fread($a, 100);
        echo "  reader: ", json_encode($r), "\n";
    }]);
    echo "  next manage()\n";
    (new Loop)->manage(fn() => null);
    gc_collect_cycles();                      // (in a cycle, it is garbage from here)
    echo "  stream: ", get_resource_type($a), "\n";
    fclose($b);

    // fclose() in the finally block of a coroutine being destroyed, while another
    // reads: it can't wait, so the reader's read fails and the stream stays open
    // until its last reference goes.
    echo "fclose() while unwinding, another reading ($how)\n";
    [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    destroy_while_waiting(new Loop, $how, function () use ($a) {
        try { usleep(5000000); } finally { echo "  fclose: ", json_encode(fclose($a)), "\n"; }
    }, before: [function () use ($a) {
        $r = fread($a, 100);
        echo "  reader: ", json_encode($r), "\n";
    }]);
    echo "  stream: ", get_resource_type($a), "\n";
    fclose($a);
    echo "  closed: ", get_resource_type($a), "\n";
    fclose($b);
}
?>
--EXPECT--
reader destroyed while another closes (refcount)
  finally ran
  closer: true, Unknown
closer destroyed while another reads (refcount)
  reader: false
  next manage()
  finally ran
  stream: Unknown
fclose() while unwinding, another reading (refcount)
  fclose: true
  finally ran
  reader: false
  stream: stream
  closed: Unknown
reader destroyed while another closes (gc)
  finally ran
  collected: yes
  closer: true, Unknown
closer destroyed while another reads (gc)
  collected: no
  reader: false
  next manage()
  finally ran
  stream: Unknown
fclose() while unwinding, another reading (gc)
  fclose: true
  finally ran
  collected: yes
  reader: false
  stream: stream
  closed: Unknown