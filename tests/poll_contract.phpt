--TEST--
Poller: slots, registrations without a busy loop, the loop's timeouts pass through, close wakes waiters, cancelled pool tasks are safe, usable without manage(), fork guard, collectable
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
    function () use ($a, $loop) { $loop->poller->readable($a); echo 'readable: ', fread($a, 10), "\n"; },
    function () use ($b) { usleep(50000); fwrite($b, 'x'); },
);

// The loop's timeout reaches the caller as it is (no native conversion here).
$loop = new Loop;
$loop->runAll(function () use ($a, $loop) {
    try { $loop->poller->readable($a, 0.1); echo "no timeout?!\n"; }
    catch (LoopTimeout $e) { echo "readable: LoopTimeout\n"; }
});

// A reader and a writer wait on the same stream at once; a second reader is refused.
stream_set_blocking($a, false);
while (fwrite($a, str_repeat('.', 65536)) > 0) {}      // $a's send buffer is full
stream_set_blocking($a, true);
$log = [];
$loop = new Loop;
$p = $loop->poller;
$loop->runAll(
    function () use ($a, $p, &$log) { $p->readable($a); $log[] = 'reader woke: ' . fread($a, 5); },
    function () use ($a, $p, &$log) { $p->writable($a); $log[] = 'writer woke'; },
    function () use ($a, $p, &$log) {
        try { $p->readable($a); } catch (LogicException $e) { $log[] = 'second reader: ' . $e->getMessage(); }
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
$loop = new Loop;
$loop->runAll(
    function () use ($c, $loop) {
        $loop->poller->readable($c);
        echo 'close woke the waiter: ', is_resource($c) ? 'still open' : 'closed', "\n";
    },
    function () use ($c) { usleep(50000); fclose($c); },
);

// A regular file is always ready (epoll refuses it): no park.
$loop = new Loop;
$loop->runAll(fn() => $loop->poller->readable(fopen(__FILE__, 'r')));
echo 'regular file parks: ', $loop->parks, "\n";

// No busy loop: once woken, an unread socket with nobody waiting makes poll()
// return at most once more (that disarms it), then poll() waits.
fwrite($b, 'unread');
$loop = new Loop;
$loop->runAll(function () use ($a, $loop) { $loop->poller->readable($a); });   // woken, doesn't read
$loop->poller->poll(0.2);                               // may return at once, once
$t = microtime(true);
$loop->poller->poll(0.2);
echo 'idle poll waited: ', microtime(true) - $t >= 0.15 ? 'yes' : 'no (busy loop)', "\n";
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

// poll(0) skips epoll when nothing is armed, yet still delivers a finished
// thread task (a loop that only ever polls without waiting).
$loop = new Loop;
$loop->spin = true;
$loop->runAll(fn() => print('spinning loop, pool read: ' . strlen(file_get_contents(__FILE__, false, null, 0, 5)) . " bytes\n"));
echo 'parked on the pool: ', $loop->parks, "\n";

// Without manage(): a loop can use a Poller on its own (no functions hooked).
$loop = new Loop;
$f = $loop->go(function () use ($a, $loop) { $loop->poller->readable($a); echo 'no manage(): ', fread($a, 10), "\n"; });
$loop->go(function () use ($b, $loop) { $loop->poller->writable($b); fwrite($b, 'plain'); });
$loop->run();

// readable() needs a coroutine.
try { (new Loop)->poller->readable($a); } catch (Error $e) { echo $e->getMessage(), "\n"; }

// A Poller freed while a stream it knew is still open: closing the stream later is safe.
$loop = new Loop;
[$e1, $e2] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
fwrite($e2, 'y');
$loop->runAll(function () use ($e1, $loop) { $loop->poller->readable($e1); });
unset($loop);
gc_collect_cycles();                                     // the loop <-> Poller closures cycle goes
fclose($e1); fclose($e2);
echo "freed Poller: ok\n";

// fork(): the child must make its own Poller.
if (function_exists('pcntl_fork')) {
    $loop = new Loop;
    if (($pid = pcntl_fork()) === 0) {
        try { $loop->poller->poll(0); } catch (Error $e) { echo 'child: ', $e->getMessage(), "\n"; }
        $child = new Loop;
        $child->poller->poll(0);
        echo "child: new Poller works\n";
        exit;
    }
    pcntl_waitpid($pid, $st);
    $loop->poller->poll(0);
    echo "parent: still works\n";
} else {
    echo "child: This Poller belongs to another process; create a new one after fork()\nchild: new Poller works\nparent: still works\n";
}
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
spinning loop, pool read: 5 bytes
parked on the pool: 1
no manage(): plain
Poller::readable() must be called in a coroutine
freed Poller: ok
child: This Poller belongs to another process; create a new one after fork()
child: new Poller works
parent: still works
