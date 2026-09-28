--TEST--
virtualize() inside manage(): two concurrent requests on the same session take turns on its lock without blocking the worker, as under php-fpm
--EXTENSIONS--
phasync
session
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--INI--
session.use_strict_mode=0
session.use_only_cookies=1
session.cache_limiter=
--FILE--
<?php
require __DIR__ . '/loop.inc';
require __DIR__ . '/sink.inc';
use function phasync\ext\virtualize;

$dir = sys_get_temp_dir() . '/phasync_sesslock_' . getmypid();
@mkdir($dir);
ini_set('session.save_path', $dir);
$_COOKIE = ['PHPSESSID' => 'sharedsession123'];

$loop = new Loop;
$log = [];
$ticks = 0;
$done = 0;
// A holds the session for 200ms; B, arriving meanwhile, waits for A's write.
$loop->go(function () use ($loop, &$log, &$done) {
    virtualize(function () use ($loop, &$log) {
        session_start();
        $log[] = 'A has the session';
        $loop->sleep(200000);
        $_SESSION['by'] = 'A';
        $log[] = 'A done';
    }, new Sink);
    $done++;
});
$loop->go(function () use ($loop, &$log, &$done) {
    $loop->sleep(20000);
    virtualize(function () use (&$log) {
        $log[] = 'B asks for the session';
        session_start();
        $log[] = 'B has the session, written by ' . ($_SESSION['by'] ?? 'nobody');
    }, new Sink);
    $done++;
});
$loop->go(function () use ($loop, &$ticks, &$done) {
    while ($done < 2) { $loop->sleep(10000); $ticks++; }
});
$loop->manage(fn() => $loop->run());
echo implode("\n", $log), "\n";
echo $ticks >= 10 ? "the worker kept running\n" : "BLOCKED (ticks=$ticks)\n";

array_map('unlink', glob("$dir/sess_*"));
rmdir($dir);
?>
--EXPECT--
A has the session
B asks for the session
A done
B has the session, written by A
the worker kept running
