--TEST--
fopen() regular file POOL-wrapped: fread offloads to the pool; handler gets a resource
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
$path = tempnam(sys_get_temp_dir(), 'phasync_pool_');
file_put_contents($path, "regular-file-contents-0123456789");

\phasync\ext\manage(function () use ($path) {
    $drive = function (Fiber $f) {
        $res = $f->start();
        while (!$f->isTerminated()) {
            $r = [$res]; $w = $e = null;
            \phasync\ext\stream_select($r, $w, $e, 5);
            $res = $f->resume();
        }
    };
    $f = new Fiber(function () use ($path) {
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
    $drive($f);
},
fn($res) => Fiber::suspend($res),
fn($res) => Fiber::suspend($res),
fn($us) => Fiber::suspend($us));

unlink($path);
echo "done\n";
?>
--EXPECT--
data=regular-file-contents-0123456789
done
