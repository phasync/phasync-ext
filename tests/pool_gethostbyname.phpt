--TEST--
Hooked gethostbyname() resolves on the thread pool and parks the coroutine
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
$loop->runAll(fn() => print("ip=" . gethostbyname('localhost') . "\n"));
var_dump($loop->parks);   // int(1): resolved on the pool
echo "done\n";
?>
--EXPECT--
ip=127.0.0.1
int(1)
done
