--TEST--
Regression: default_socket_timeout applies inside manage() exactly where native PHP applies it (socket streams), stream_set_timeout() overrides it, and pipes still wait without one
--EXTENSIONS--
phasync
--INI--
default_socket_timeout=1
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
?>
--FILE--
<?php
// PHP gives every socket stream it opens FG(default_socket_timeout) as its read timeout;
// files and pipes get none. The extension must hand park() the same timeout, so a read
// that times out natively times out the same way in a coroutine, and nothing else does.
require __DIR__ . '/loop.inc';

/** fread() on $stream, natively and in a coroutine: what it returned, timed_out, and whether it
 * took about $seconds (not less, and not much more: CI machines are slow). */
function both(string $label, float $seconds, Closure $open): void {
    foreach (['native' => false, 'coroutine' => true] as $how => $inLoop) {
        [$stream, $cleanup] = $open();
        $t    = microtime(true);
        $read = null;
        $run  = function () use ($stream, &$read) { $read = fread($stream, 10); };
        if ($inLoop) {
            $loop = new Loop;
            $loop->runAll($run);
            $given = $loop->parkTimeouts[0] ?? null;
        } else {
            $run();
            $given = '-';
        }
        $took = microtime(true) - $t;
        printf("%s, %s: %s, timed_out=%s, %s, park timeout %s\n", $label, $how,
            var_export($read, true), var_export(stream_get_meta_data($stream)['timed_out'], true),
            $took >= $seconds - 0.05 && $took < $seconds + 0.5 ? "after $seconds s" : sprintf('after %.2f s (expected %s)', $took, $seconds),
            is_float($given) ? sprintf('%.1f', $given) : var_export($given, true));
        $cleanup();
    }
}

$server = stream_socket_server('tcp://127.0.0.1:0');
$addr   = stream_socket_get_name($server, false);
[$host, $port] = explode(':', $addr);

// A connected client whose peer never writes
both('stream_socket_client', 1.0, function () use ($server, $addr) {
    $c = stream_socket_client("tcp://$addr");
    $a = stream_socket_accept($server);
    return [$c, function () use ($c, $a) { fclose($c); fclose($a); }];
});
both('fsockopen', 1.0, function () use ($server, $host, $port) {
    $c = fsockopen($host, (int) $port);
    $a = stream_socket_accept($server);
    return [$c, function () use ($c, $a) { fclose($c); fclose($a); }];
});
// The accepted side, whose peer never writes
both('accepted', 1.0, function () use ($server, $addr) {
    $c = stream_socket_client("tcp://$addr");
    $a = stream_socket_accept($server);
    return [$a, function () use ($c, $a) { fclose($c); fclose($a); }];
});
// stream_set_timeout() overrides default_socket_timeout
both('stream_set_timeout(0.3)', 0.3, function () use ($server, $addr) {
    $c = stream_socket_client("tcp://$addr");
    $a = stream_socket_accept($server);
    stream_set_timeout($c, 0, 300000);
    return [$c, function () use ($c, $a) { fclose($c); fclose($a); }];
});
// A pipe gets no timeout: it waits for the child's output, 1.5 s, past default_socket_timeout
both('pipe', 1.5, function () {
    $p = proc_open([PHP_BINARY, '-n', '-r', 'usleep(1500000); echo "late";'], [1 => ['pipe', 'w']], $pipes);
    return [$pipes[1], function () use ($p, $pipes) { fclose($pipes[1]); proc_close($p); }];
});
?>
--EXPECTF--
stream_socket_client, native: false, timed_out=true, after 1 s, park timeout '-'
stream_socket_client, coroutine: false, timed_out=true, after 1 s, park timeout 1.0
fsockopen, native: false, timed_out=true, after 1 s, park timeout '-'
fsockopen, coroutine: false, timed_out=true, after 1 s, park timeout 1.0
accepted, native: false, timed_out=true, after 1 s, park timeout '-'
accepted, coroutine: false, timed_out=true, after 1 s, park timeout 1.0
stream_set_timeout(0.3), native: false, timed_out=true, after 0.3 s, park timeout '-'
stream_set_timeout(0.3), coroutine: false, timed_out=true, after 0.3 s, park timeout 0.3
pipe, native: 'late', timed_out=false, after 1.5 s, park timeout '-'
pipe, coroutine: 'late', timed_out=false, after 1.5 s, park timeout NULL
