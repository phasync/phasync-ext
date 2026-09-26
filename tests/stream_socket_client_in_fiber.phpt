--TEST--
stream_socket_client() connect/DNS suspend the fiber inside a scope; timeouts, refusals and DNS errors match native
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
final class TestTimeout extends Exception {}
// A listener whose accept queue is full: Linux drops new SYNs, so a connect to it
// hangs until the client retries or times out. Loopback only, so it's deterministic.
function fullListener(array &$keep): string {
    $srv = stream_socket_server('tcp://127.0.0.1:0', $e, $es, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
        stream_context_create(['socket' => ['backlog' => 0]]));
    $addr = stream_socket_get_name($srv, false);
    $keep[] = $srv;
    for ($i = 0; $i < 4; $i++) {
        $keep[] = stream_socket_client("tcp://$addr", $en, $es, 1, STREAM_CLIENT_CONNECT | STREAM_CLIENT_ASYNC_CONNECT);
    }
    usleep(100000);
    return $addr;
}
// Handlers that wait like phasync does: in the right direction, up to $t, then
// signal a timeout. (DNS runs on the worker pool and waits via the READ handler.)
function waiter(bool $write): Closure {
    return function ($s, $t) use ($write) {
        $r = $write ? null : [$s]; $w = $write ? [$s] : null; $e = null;
        if (\phasync\ext\stream_select($r, $w, $e, $t === null ? 5 : 0, $t === null ? 0 : (int) ($t * 1e6)) < 1) {
            throw new TestTimeout();
        }
    };
}
$rd = waiter(false);
$wr = waiter(true);
function connectBoth(string $url, float $timeout, Closure $rd, Closure $wr): void {
    $native = @stream_socket_client($url, $en1, $es1, $timeout);
    $w1 = preg_replace('/^.*?\(\): /', '', error_get_last()['message'] ?? '');
    error_clear_last();
    $res = [];
    $body = function () use ($url, $timeout, &$res) {       // outside the arrow fn: &$res binds here
        $c = @stream_socket_client($url, $en, $es, $timeout);
        $res = [$c, $en, $es, preg_replace('/^.*?\(\): /', '', error_get_last()['message'] ?? '')];
    };
    (new Fiber(fn() => \phasync\ext\manage($body, $rd, $wr, fn($us) => null, TestTimeout::class)))->start();
    [$c2, $en2, $es2, $w2] = $res;
    printf("%s | native: %s %d %s | ext: %s %d %s | same warning: %s\n",
        preg_replace('/\d+$/', 'PORT', $url), var_export($native, true), $en1, $es1,
        var_export($c2, true), $en2, $es2, var_export($w1 === $w2, true));
}

// 1. Parity on failure paths.
$keep = [];
$addr = fullListener($keep);
connectBoth("tcp://$addr", 0.3, $rd, $wr);                       // timeout
connectBoth('tcp://127.0.0.1:1', 1, $rd, $wr);                   // refused
connectBoth('tcp://no-such-host.invalid:80', 1, $rd, $wr);       // DNS failure

// 2. Concurrency: A's connect hangs on the full queue while the scheduler keeps
//    running; the scheduler then drains the queue and A's SYN retry gets through.
$keep2 = [];
$addr2 = fullListener($keep2);
$log = [];
\phasync\ext\manage(function () use ($addr2, &$log) {
    $a = new Fiber(function () use ($addr2, &$log) {
        $c = stream_socket_client("tcp://$addr2", $en, $es, 5);
        $log[] = 'A: ' . (is_resource($c) ? 'connected' : "failed $es");
    });
    $res = $a->start();                                          // parked: write wait
    $log[] = 'A parked: ' . var_export(is_resource($res), true);
    usleep(300000);
    $log[] = 'scheduler ran while A was connecting';
    while ($x = @stream_socket_accept($GLOBALS['keep2'][0], 0)) { $GLOBALS['drained'][] = $x; }
    $r = null; $w = [$res]; $e = null;
    \phasync\ext\stream_select($r, $w, $e, 4);
    $a->resume();
}, fn($s, $t) => Fiber::suspend($s), fn($s, $t) => Fiber::suspend($s), fn($us) => null, TestTimeout::class);
echo implode("\n", $log), "\n";

// 3. "localhost" against an IPv4-only server connects (native tries ::1 first
//    where it's listed, then falls back to 127.0.0.1).
$v4 = stream_socket_server('tcp://127.0.0.1:0');
$port = parse_url('tcp://' . stream_socket_get_name($v4, false), PHP_URL_PORT);
(new Fiber(fn() => \phasync\ext\manage(function () use ($port) {
    $c = stream_socket_client("tcp://localhost:$port", $en, $es, 2);
    echo 'localhost: ', is_resource($c) ? 'connected' : "failed $es", "\n";
}, $rd, $wr, fn($us) => null, TestTimeout::class)))->start();
?>
--EXPECTF--
tcp://127.0.0.1:PORT | native: false 110 Connection timed out | ext: false 110 Connection timed out | same warning: true
tcp://127.0.0.1:PORT | native: false 111 Connection refused | ext: false 111 Connection refused | same warning: true
tcp://no-such-host.invalid:PORT | native: false %d %s | ext: false %d %s | same warning: true
A parked: true
scheduler ran while A was connecting
A: connected
localhost: connected
