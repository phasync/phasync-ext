--TEST--
Regression: pclose() by one coroutine while another reads its pipe wakes it (EBADF) like fclose() already does (#14, #23); unset() of what looks like the closer's own last reference does not disturb a coroutine still reading (it is not really the last one: the reader holds its own for as long as it is inside its own operation); no descriptors leak once the reader is gone either way
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';

// Warm up what the extension itself opens once, lazily, on first use (the
// Poller's epoll and eventfd, /proc/self/mountinfo for fs offload detection):
// unrelated to this test, but counted as a leak below if not opened already.
(new Loop)->runAll(function () {
    $p = popen('true', 'r');
    fread($p, 100);
    pclose($p);
});

// Loop/Poller (the test harness) hold each other in a reference cycle: a
// collection makes sure a completed one is actually gone before counting.
function fd_count(): int
{
    gc_collect_cycles();
    return count(scandir('/proc/self/fd')) - 2;   // '.' and '..'
}

function race(string $name, Closure $op, Closure $close): void
{
    $log = [];
    (new Loop)->runAll(
        function () use ($op, &$log) { $log[] = 'op: ' . json_encode($op()); },
        function () use ($close, &$log) { usleep(20000); $close(); $log[] = 'closed'; },
    );
    printf("%-8s %s\n", $name, implode(', ', $log));
}

$before = fd_count();
$p = popen('sleep 0.2; yes 2>/dev/null', 'r');
race('pclose', fn() => fread($p, 100), fn() => pclose($p));
$after = fd_count();
echo "fds: ", $after <= $before ? 'no leak' : "LEAK ($before -> $after)", "\n";

// unset($a) here drops only the closer's own reference: the reader, still
// inside its own fread($a, ...) call, holds another (its call keeps its own
// copy of the argument alive for as long as the call runs) -- as does the main
// scope's own $a below, kept on purpose to show it. So this unset() does not
// close the stream at all: no wake, no EBADF, the write reaches the reader.
$before = fd_count();
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
$log = [];
(new Loop)->runAll(
    function () use ($a, &$log) { $log[] = 'op: ' . json_encode(fread($a, 100)); },
    function () use ($a, $b, &$log) { usleep(20000); fwrite($b, 'hi'); unset($a); $log[] = 'unset'; },
);
printf("%-8s %s\n", 'unset', implode(', ', $log));
echo "a: ", get_resource_type($a), "\n";
fclose($a);
fclose($b);
$after = fd_count();
echo "fds: ", $after <= $before ? 'no leak' : "LEAK ($before -> $after)", "\n";
?>
--EXPECT--
pclose   op: false, closed
fds: no leak
unset    unset, op: "hi"
a: stream
fds: no leak
