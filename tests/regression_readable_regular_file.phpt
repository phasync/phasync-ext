--TEST--
Poller::readable()/writable() on a regular file wait for the loop's next poll(), as phasync's own wait does without the extension (#29); hooked reads of it don't park
--EXTENSIONS--
phasync
--FILE--
<?php
require __DIR__ . '/loop.inc';
$f = fopen(__FILE__, 'r+');

foreach (['readable', 'writable'] as $m) {
    $loop = new Loop;
    $loop->runAll(
        function () use ($f, $loop, $m) { echo "a: $m\n"; $loop->poller->$m($f); echo "a: ready\n"; },
        function () { echo "b: runs meanwhile\n"; },
    );
    echo "parks: {$loop->parks}\n";
}

// The wait can be cancelled like any other.
$loop = new Loop;
$a = new Fiber(function () use ($f, $loop) {
    try { $loop->poller->readable($f); echo "not cancelled?!\n"; }
    catch (LoopCancelled $e) { echo "cancelled\n"; }
});
$loop->runAll($a, function () use ($loop, $a) { $loop->cancel($a); });

// fread() in a scope reads a local file inline.
$loop = new Loop;
$loop->runAll(function () use ($f) { echo strlen(fread($f, 10)), "\n"; });
echo "parks: {$loop->parks}\n";
?>
--EXPECT--
a: readable
b: runs meanwhile
a: ready
parks: 1
a: writable
b: runs meanwhile
a: ready
parks: 1
cancelled
10
parks: 0
