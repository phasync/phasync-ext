--TEST--
A stream already non-blocking before it is wrapped (STDIN/STDOUT/STDERR, or anything opened
before the first manage() scope) stays non-blocking: a would-block write returns at once,
never parking (#6)
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!function_exists('posix_mkfifo')) die('skip requires posix'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;

$path = sys_get_temp_dir() . '/phasync_nb_wrap_' . getmypid() . '.fifo';
posix_mkfifo($path, 0600);
// r+: opens without waiting for a reader, and keeps this process's own read end open so the
// pipe never sees EOF.
$w = fopen($path, 'r+');
stream_set_blocking($w, false);    // non-blocking, BEFORE any manage() scope wraps it
for ($n = 0; $n < 10000 && @fwrite($w, str_repeat('x', 4096)) > 0; $n++);
if ($n === 0 || $n >= 10000) {
    echo "skip: could not fill the pipe\n";
    fclose($w);
    unlink($path);
    exit;
}
// $w is now full, non-blocking, and still unwrapped: the first manage() scope wraps it lazily
// (phasync_wrap_existing_streams()), and must pick up its real (non-blocking) mode.

$r = 'unset';
$loop->runAll(function () use ($w, &$r) {
    $r = fwrite($w, 'more data');  // would block: must return 0 at once, not park
});
var_dump($r === 0);                // bool(true): a native non-blocking write on a full pipe
var_dump($loop->parks);            // int(0): never parked

fclose($w);
unlink($path);
?>
--EXPECT--
bool(true)
int(0)
