--TEST--
fopen() regular file POOL-wrapped: fread offloads to the pool and parks the coroutine
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$path = tempnam(sys_get_temp_dir(), 'phasync_pool_');
file_put_contents($path, "regular-file-contents-0123456789");

$loop = new Loop;
$loop->runAll(function () use ($path) {
    $fh = fopen($path, 'r');          // regular file -> POOL-wrapped
    $data = '';
    while (!feof($fh)) {
        $chunk = fread($fh, 8);       // each read offloads to a worker thread
        if ($chunk === '' || $chunk === false) break;
        $data .= $chunk;
    }
    fclose($fh);
    echo "data=$data\n";
});
var_dump($loop->parks > 0);
unlink($path);
echo "done\n";
?>
--EXPECT--
data=regular-file-contents-0123456789
bool(true)
done
