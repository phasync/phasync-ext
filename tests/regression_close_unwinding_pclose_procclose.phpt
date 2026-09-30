--TEST--
Regression: pclose() and proc_close() in the finally block of a coroutine being destroyed, of a pipe another coroutine is reading, no longer fatal-error; they wake the reader and leave the pipe to its last reference, like fclose() already did (#14, #23)
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
?>
--FILE--
<?php
// Up to 0.5.0-alpha23, phasync_close_if_unwinding() (the #14/#23 "leave it to
// its last reference" trick) existed only for fclose(). pclose() and
// proc_close() had no such check: their own zend_list_close() would free a
// pipe's stream under a coroutine still reading it, exactly as fclose() would,
// and the extension raised a fatal error to avoid the use-after-free instead.
require __DIR__ . '/destroy.inc';

// Warm up what the extension itself opens once, lazily, on first use (the
// Poller's epoll and eventfd, /proc/self/mountinfo for fs offload detection):
// unrelated to this test, but counted as a leak below if not opened already.
(new Loop)->runAll(function () {
    $p = popen('true', 'r');
    fread($p, 100);
    pclose($p);
});

// Loop/Poller (the test harness) hold each other in a reference cycle (a Poller
// is built from closures bound to its Loop): a Loop only used with 'refcount'
// needs a collection to actually go, same as any other cyclic garbage, so every
// count below is taken after one -- unrelated to whatever this test is
// checking for a leak.
function fd_count(): int
{
    gc_collect_cycles();
    return count(scandir('/proc/self/fd')) - 2;   // '.' and '..'
}

foreach (['refcount', 'gc'] as $how) {
    echo "pclose() while unwinding, another reading ($how)\n";
    $before = fd_count();
    $p = popen('sleep 0.3; true', 'r');
    destroy_while_waiting(new Loop, $how, function () use ($p) {
        try { usleep(200000); } finally { echo "  pclose: ", json_encode(pclose($p)), "\n"; }
    }, before: [function () use ($p) {
        echo "  reader: ", json_encode(fread($p, 100)), "\n";
    }]);
    echo "  stream: ", get_resource_type($p), "\n";
    fclose($p);
    echo "  closed: ", get_resource_type($p), "\n";
    $after = fd_count();
    echo "  fds: ", $after <= $before ? 'no leak' : "LEAK ($before -> $after)", "\n";

    echo "proc_close() while unwinding, another reading ($how)\n";
    $before = fd_count();
    $proc = proc_open(['sleep', '0.3'], [1 => ['pipe', 'w']], $pipes);
    destroy_while_waiting(new Loop, $how, function () use ($proc) {
        try { usleep(200000); } finally { echo "  proc_close: ", json_encode(proc_close($proc)), "\n"; }
    }, before: [function () use ($pipes) {
        echo "  reader: ", json_encode(fread($pipes[1], 100)), "\n";
    }]);
    echo "  stream: ", get_resource_type($pipes[1]), "\n";
    fclose($pipes[1]);
    echo "  closed: ", get_resource_type($pipes[1]), "\n";
    $after = fd_count();
    echo "  fds: ", $after <= $before ? 'no leak' : "LEAK ($before -> $after)", "\n";
}
?>
--EXPECT--
pclose() while unwinding, another reading (refcount)
  reader:   pclose: -1
  finally ran
false
  stream: stream
  closed: Unknown
  fds: no leak
proc_close() while unwinding, another reading (refcount)
  reader:   proc_close: -1
  finally ran
false
  stream: stream
  closed: Unknown
  fds: no leak
pclose() while unwinding, another reading (gc)
  reader:   pclose: -1
  finally ran
  collected: yes
false
  stream: stream
  closed: Unknown
  fds: no leak
proc_close() while unwinding, another reading (gc)
  reader:   proc_close: -1
  finally ran
  collected: yes
false
  stream: stream
  closed: Unknown
  fds: no leak
