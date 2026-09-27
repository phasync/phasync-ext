--TEST--
poll()/readable()/writable(): slots, one-shot registrations, the loop's timeouts pass through, close wakes waiters, cancelled pool tasks are safe
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);

// readable() parks until data arrives.
$loop = new Loop;
$loop->runAll(
    function () use ($a) { \phasync\ext\readable($a); echo 'readable: ', fread($a, 10), "\n"; },
    function () use ($b) { usleep(50000); fwrite($b, 'x'); },
);

// The loop's timeout reaches the caller as it is (no native conversion here).
(new Loop)->runAll(function () use ($a) {
    try { \phasync\ext\readable($a, 0.1); echo "no timeout?!\n"; }
    catch (LoopTimeout $e) { echo "readable: LoopTimeout\n"; }
});

// A reader and a writer wait on the same stream at once; a second reader is refused.
stream_set_blocking($a, false);
while (fwrite($a, str_repeat('.', 65536)) > 0) {}      // $a's send buffer is full
stream_set_blocking($a, true);
$log = [];
(new Loop)->runAll(
    function () use ($a, &$log) { \phasync\ext\readable($a); $log[] = 'reader woke: ' . fread($a, 5); },
    function () use ($a, &$log) { \phasync\ext\writable($a); $log[] = 'writer woke'; },
    function () use ($a, &$log) {
        try { \phasync\ext\readable($a); } catch (LogicException $e) { $log[] = 'second reader: ' . $e->getMessage(); }
    },
    function () use ($b, &$log) {
        usleep(50000);
        $log[] = 'B writes';
        fwrite($b, 'hello');                             // wakes the reader, not the writer
        usleep(50000);
        $log[] = 'B drains';
        stream_set_blocking($b, false);
        while (fread($b, 65536) !== '') {}               // makes room: wakes the writer
    },
);
echo implode("\n", $log), "\n";

// Closing a stream wakes whoever waits on it; the next read finds it closed.
[$c, $d] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
(new Loop)->runAll(
    function () use ($c) {
        \phasync\ext\readable($c);
        echo 'close woke the waiter: ', is_resource($c) ? 'still open' : 'closed', "\n";
    },
    function () use ($c) { usleep(50000); fclose($c); },
);

// A regular file is always ready (epoll refuses it): no park.
$loop = new Loop;
$loop->runAll(fn() => \phasync\ext\readable(fopen(__FILE__, 'r')));
echo 'regular file parks: ', $loop->parks, "\n";

// One-shot: once woken, an unread socket with nobody waiting doesn't make
// poll() return at once (no busy loop).
fwrite($b, 'unread');
$loop = new Loop;
$loop->runAll(function () use ($a) { \phasync\ext\readable($a); });   // woken, doesn't read
$loop->manage(function () {
    $t = microtime(true);
    \phasync\ext\poll(0.2);
    echo 'idle poll waited: ', microtime(true) - $t >= 0.15 ? 'yes' : 'no (busy loop)', "\n";
});
fread($a, 100);

// Cancelling a coroutine whose pool task is running: the cancellation arrives
// once the task is done, and its late completion only finds a vacant slot.
$loop = new Loop;
$loop->runAll(
    function () {
        $GLOBALS['dns'] = Fiber::getCurrent();
        try { gethostbyname('localhost'); echo "resolved?!\n"; }
        catch (LoopCancelled $e) { echo "pool task: cancelled\n"; }
    },
    function () use ($loop) { $loop->cancel($GLOBALS['dns']); },
    function () { usleep(100000); echo 'loop still fine: ', gethostbyname('localhost'), "\n"; },
);

// poll() needs manage(); readable() needs a coroutine inside manage().
try { \phasync\ext\poll(0); } catch (Error $e) { echo $e->getMessage(), "\n"; }
try { (new Loop)->manage(fn() => \phasync\ext\readable($a)); } catch (Error $e) { echo $e->getMessage(), "\n"; }
?>
--EXPECT--
readable: x
readable: LoopTimeout
second reader: Another coroutine is already waiting to read from this stream
B writes
reader woke: hello
B drains
writer woke
close woke the waiter: closed
regular file parks: 0
idle poll waited: yes
pool task: cancelled
loop still fine: 127.0.0.1
phasync\ext\poll() must be called inside manage()
phasync\ext\readable() must be called in a coroutine inside manage()
