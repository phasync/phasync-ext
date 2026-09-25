--TEST--
Timeout contract: native timeout passed to the handler (remaining time on retries, null = forever); a thrown timeout-class exception finishes the op the native way
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

// Run $fn in a fiber inside a scope whose read/write handler is $h.
function scoped(Closure $fn, Closure $h): void {
    \phasync\ext\manage(fn() => (new Fiber($fn))->start(), $h, $h, fn($us) => null, TestTimeout::class);
}

[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
stream_set_timeout($a, 3);

// 1. The handler is handed the stream's own timeout. A spurious wake-up (the
//    handler returns without data) is retried with only the REMAINING time.
//    Throwing the timeout class ends the wait: fgets() returns the partial line,
//    timed_out is set, and no exception escapes.
$seen = [];
fwrite($b, 'partial');
scoped(function () use ($a) {
    var_dump(fgets($a));                               // string(7) "partial"
    var_dump(stream_get_meta_data($a)['timed_out']);   // bool(true)
}, function ($res, $t) use (&$seen) {
    $seen[] = $t;
    if (count($seen) === 1) { usleep(50000); return; } // spurious wake
    throw new TestTimeout();
});
var_dump($seen[0]);                                     // float(3)
var_dump($seen[1] < 3.0 && $seen[1] > 2.5);             // bool(true): remaining time

// 2. A new call starts a fresh, full timeout; fread() on an empty stream that
//    times out returns false.
$seen = [];
scoped(function () use ($a) {
    var_dump(fread($a, 100));                          // bool(false)
    var_dump(stream_get_meta_data($a)['timed_out']);   // bool(true)
}, function ($res, $t) use (&$seen) { $seen[] = $t; throw new TestTimeout(); });
var_dump($seen[0]);                                     // float(3)

// 3. Where native PHP would wait forever (a pipe), the handler gets null.
$p = proc_open([PHP_BINARY, '-n', '-r', 'usleep(300000);'], [1 => ['pipe', 'w']], $pipes);
$seen = [];
scoped(function () use ($pipes) { fread($pipes[1], 10); },
       function ($res, $t) use (&$seen) { $seen[] = $t; throw new TestTimeout(); });
var_dump($seen[0]);                                     // NULL
fclose($pipes[1]); proc_close($p);

// 4. A timed-out write behaves like native: the "Send of N bytes failed" notice,
//    false, timed_out set.
[$c, $d] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
stream_set_blocking($c, false);
$chunk = str_repeat('x', 65536);
while (fwrite($c, $chunk) > 0) {}                       // fill the send buffer
stream_set_blocking($c, true);
stream_set_timeout($c, 5);
scoped(function () use ($c) {
    var_dump(fwrite($c, 'y'));                         // notice + bool(false)
    var_dump(stream_get_meta_data($c)['timed_out']);   // bool(true)
}, fn($res, $t) => throw new TestTimeout());

fclose($a); fclose($b); fclose($c); fclose($d);
?>
--EXPECTF--
string(7) "partial"
bool(true)
float(3)
bool(true)
bool(false)
bool(true)
float(3)
NULL

Notice: fwrite(): Send of 1 bytes failed with errno=11 Resource temporarily unavailable in %s on line %d
bool(false)
bool(true)
