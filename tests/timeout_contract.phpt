--TEST--
Timeout contract: park() gets the native timeout (remaining time on retries, none = forever); the loop timing it out finishes the op the native way
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
stream_set_timeout($a, 0, 300000);

// 1. park() gets the stream's own timeout. A spurious wake-up (park() returns
//    without data) is retried with only the REMAINING time. When the time runs
//    out, the loop's timeout ends the wait: fgets() returns the partial line,
//    timed_out is set, and no exception escapes.
fwrite($b, 'partial');
$loop = new Loop;
$n = 0;
$loop->onPark = function () use (&$n) { return ++$n === 1; };  // the first wake is spurious
$loop->runAll(function () use ($a) {
    var_dump(fgets($a));                               // string(7) "partial"
    var_dump(stream_get_meta_data($a)['timed_out']);   // bool(true)
});
var_dump($loop->parkTimeouts[0]);                       // float(0.3)
var_dump($loop->parkTimeouts[1] < 0.3 && $loop->parkTimeouts[1] > 0.2); // remaining time

// 2. A new call starts a fresh, full timeout; fread() on an empty stream that
//    times out returns false.
$loop = new Loop;
$loop->runAll(function () use ($a) {
    var_dump(fread($a, 100));                          // bool(false)
    var_dump(stream_get_meta_data($a)['timed_out']);   // bool(true)
});
var_dump($loop->parkTimeouts[0]);                       // float(0.3)

// 3. Where native PHP would wait forever (a pipe), park() gets no timeout.
$p = proc_open([PHP_BINARY, '-n', '-r', 'usleep(300000);'], [1 => ['pipe', 'w']], $pipes);
$loop = new Loop;
$loop->runAll(fn() => fread($pipes[1], 10));
var_dump($loop->parkTimeouts[0]);                       // NULL
fclose($pipes[1]); proc_close($p);

// 4. A timed-out write behaves like native: the "Send of N bytes failed" notice,
//    false, timed_out set.
[$c, $d] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
stream_set_blocking($c, false);
$chunk = str_repeat('x', 65536);
while (fwrite($c, $chunk) > 0) {}                       // fill the send buffer
stream_set_blocking($c, true);
stream_set_timeout($c, 0, 200000);
(new Loop)->runAll(function () use ($c) {
    var_dump(fwrite($c, 'y'));                         // notice + bool(false)
    var_dump(stream_get_meta_data($c)['timed_out']);   // bool(true)
});

fclose($a); fclose($b); fclose($c); fclose($d);
?>
--EXPECTF--
string(7) "partial"
bool(true)
float(0.3)
bool(true)
bool(false)
bool(true)
float(0.3)
NULL

Notice: fwrite(): Send of 1 bytes failed with errno=11 Resource temporarily unavailable in %s on line %d
bool(false)
bool(true)
