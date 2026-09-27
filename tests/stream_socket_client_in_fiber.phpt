--TEST--
stream_socket_client() connect/DNS park the coroutine inside a scope; timeouts, refusals and DNS errors match native
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
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
function connectBoth(string $url, float $timeout): void {
    $native = @stream_socket_client($url, $en1, $es1, $timeout);
    $w1 = preg_replace('/^.*?\(\): /', '', error_get_last()['message'] ?? '');
    error_clear_last();
    $res = [];
    (new Loop)->runAll(function () use ($url, $timeout, &$res) {
        $c = @stream_socket_client($url, $en, $es, $timeout);
        $res = [$c, $en, $es, preg_replace('/^.*?\(\): /', '', error_get_last()['message'] ?? '')];
    });
    [$c2, $en2, $es2, $w2] = $res;
    printf("%s | native: %s %d %s | ext: %s %d %s | same warning: %s\n",
        preg_replace('/\d+$/', 'PORT', $url), var_export($native, true), $en1, $es1,
        var_export($c2, true), $en2, $es2, var_export($w1 === $w2, true));
}

// 1. Parity on failure paths.
$keep = [];
$addr = fullListener($keep);
connectBoth("tcp://$addr", 0.3);                       // timeout
connectBoth('tcp://127.0.0.1:1', 1);                   // refused
connectBoth('tcp://no-such-host.invalid:80', 1);       // DNS failure

// 2. Concurrency: A's connect hangs on the full queue while B keeps running; B
//    then keeps draining the queue until one of A's SYN retries gets through (the
//    other queued clients' retries compete for the freed slot).
$keep2 = [];
$addr2 = fullListener($keep2);
$log = [];
$done = false;
(new Loop)->runAll(
    function () use ($addr2, &$log, &$done) {
        $c = stream_socket_client("tcp://$addr2", $en, $es, 8);
        $log[] = 'A: ' . (is_resource($c) ? 'connected' : "failed $es");
        $done = true;
    },
    function () use ($keep2, &$log, &$done) {
        usleep(300000);
        $log[] = 'B ran while A was connecting';
        while (!$done) {
            while ($x = @stream_socket_accept($keep2[0], 0)) { $GLOBALS['drained'][] = $x; }
            usleep(50000);
        }
    },
);
echo implode("\n", $log), "\n";

// 3. "localhost" against an IPv4-only server connects (native tries ::1 first
//    where it's listed, then falls back to 127.0.0.1).
$v4 = stream_socket_server('tcp://127.0.0.1:0');
$port = parse_url('tcp://' . stream_socket_get_name($v4, false), PHP_URL_PORT);
(new Loop)->runAll(function () use ($port) {
    $c = stream_socket_client("tcp://localhost:$port", $en, $es, 2);
    echo 'localhost: ', is_resource($c) ? 'connected' : "failed $es", "\n";
});
?>
--EXPECTF--
tcp://127.0.0.1:PORT | native: false 110 %s timed out | ext: false 110 %s timed out | same warning: true
tcp://127.0.0.1:PORT | native: false 111 Connection refused | ext: false 111 Connection refused | same warning: true
tcp://no-such-host.invalid:PORT | native: false %d %s | ext: false %d %s | same warning: true
B ran while A was connecting
A: connected
localhost: connected
