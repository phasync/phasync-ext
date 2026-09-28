--TEST--
phasync.fs_offload=network: files on a filesystem that refuses RWF_NOWAIT (procfs; ZFS ...) read inline, as natively, with native data and positions
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!is_readable('/proc/self/mountinfo')) die('skip needs /proc');
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
// procfs refuses RWF_NOWAIT everywhere; the test's own directory may too (ZFS).
$files = ['/proc/self/mountinfo', '/proc/self/limits', __FILE__, __DIR__ . '/loop.inc'];
$work = function () use ($files) {
    $r = [];
    foreach ($files as $f) {
        for ($i = 0; $i < 2; $i++) {     // the refusal is remembered: the second reads plainly
            $r[] = md5(file_get_contents($f));
            $fp = fopen($f, 'r');
            $lines = [];
            while (($l = fgets($fp)) !== false) $lines[] = $l;
            $r[] = [count($lines), md5(implode('', $lines)), ftell($fp), feof($fp)];
            fclose($fp);
        }
    }
    $fp = fopen(__FILE__, 'r');
    fseek($fp, 10);
    $r[] = [fread($fp, 4), ftell($fp)];
    fclose($fp);
    return $r;
};
$native = $work();
$p = $loop->parks;
$in = null;
$loop->runAll(function () use ($work, &$in) { $in = $work(); });
echo 'coroutine: ', $in === $native ? 'same as native' : 'DIFFERENT', ', ', $loop->parks > $p ? 'pooled' : 'inline', "\n";
var_dump(end($native));
?>
--EXPECT--
coroutine: same as native, inline
array(2) {
  [0]=>
  string(4) "ire "
  [1]=>
  int(14)
}
