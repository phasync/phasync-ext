--TEST--
In a coroutine being destroyed (its finally blocks running), an operation that would wait fails with the FiberError Fiber::suspend() throws there, without reaching the loop; a thread-pool operation runs inline
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--INI--
phasync.fs_offload=all
--FILE--
<?php
require __DIR__ . '/destroy.inc';

$file = tempnam(sys_get_temp_dir(), 'phasync_destroy_');
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
$loop = new Loop;
destroy_while_waiting($loop, 'refcount', function () use ($a, $file) {
    try {
        fread($a, 100);
    } finally {
        foreach ([
            'fread' => fn() => fread($a, 100),
            'usleep' => fn() => usleep(1000),
            'file_put_contents' => fn() => file_put_contents($file, 'written in finally'),
            'file_get_contents' => fn() => file_get_contents($file),
        ] as $name => $op) {
            try {
                $r = $op();
                echo "  $name: ", json_encode($r), "\n";
            } catch (FiberError $e) {
                echo "  $name: ", get_class($e), ": ", $e->getMessage(), "\n";
            }
        }
    }
}, after: [function () use ($a, $b) {
    fwrite($b, 'still works');
    echo "  then read: ", fread($a, 100), "\n";
}]);
echo "parks: ", $loop->parks, "\n";        // only the destroyed read's
unlink($file);
?>
--EXPECT--
  fread: FiberError: Cannot suspend in a force-closed fiber
  usleep: FiberError: Cannot suspend in a force-closed fiber
  file_put_contents: 18
  file_get_contents: "written in finally"
  finally ran
  then read: still works
parks: 1
