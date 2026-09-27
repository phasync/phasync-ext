--TEST--
shell_exec/exec/system/passthru/popen/pclose/proc_close suspend the fiber inside a scope (others keep running) and keep native results
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (PHP_OS_FAMILY !== 'Linux') die('skip Linux only');
if (!is_readable('/proc/self/task/' . getmypid() . '/children')) die('skip requires /proc children');
if (!function_exists('proc_open')) die('skip requires proc_open');
?>
--INI--
error_reporting=E_ALL & ~E_DEPRECATED
--FILE--
<?php
require __DIR__ . '/loop.inc';

// Run $f natively, then in a scope beside a fiber ticking every 20ms: the results
// must match, and the ticker must have kept running while the child did.
function check(string $name, Closure $f): void {
    ob_start(); $native = $f(); $native = [$native, ob_get_clean()];
    $ext = null; $ticks = 0; $done = false;
    (new Loop)->runAll(
        function () use ($f, &$ext, &$done) {
            ob_start(); $r = $f(); $ext = [$r, ob_get_clean()]; $done = true;
        },
        function () use (&$ticks, &$done) {
            while (!$done) { usleep(20000); $ticks++; }
        },
    );
    printf("%-22s %s %s\n", $name, $native === $ext ? 'same' : 'DIFF ' . json_encode([$native, $ext]),
        $ticks >= 5 ? 'cooperative' : "BLOCKED (ticks=$ticks)");
}

$tmp = tempnam(sys_get_temp_dir(), 'phasync_pf_');

check('shell_exec read', fn() => shell_exec('printf a; sleep 0.3; printf b'));
// The child closes stdout and keeps running: only pclose()'s wait is left.
check('shell_exec close wait', fn() => shell_exec('printf a; exec 1>&-; sleep 0.3'));
check('exec', function () { $o = []; $last = exec('echo 1; echo 2; exec 1>&-; sleep 0.3; exit 3', $o, $rc); return [$last, $o, $rc]; });
check('system', function () { $last = system('echo hi; sleep 0.3; exit 2', $rc); return [$last, $rc]; });
check('passthru', function () { $r = passthru('printf x; sleep 0.3; exit 1', $rc); return [$r, $rc]; });
check('backticks', fn() => `sleep 0.3; echo tick`);
check('popen r + pclose', function () {
    $p = popen('sleep 0.2; echo x; exec 1>&-; sleep 0.3; exit 4', 'r');
    return [stream_get_contents($p), pclose($p)];
});
// pclose() of a child still writing: closing our end first makes it exit (EPIPE).
check('pclose unread output', function () {
    $p = popen('sleep 0.3; yes 2>/dev/null', 'r');
    return pclose($p) !== -1;
});
check('popen w + pclose', function () use ($tmp) {
    $p = popen('cat > ' . escapeshellarg($tmp) . '; sleep 0.3; exit 7', 'w');
    fwrite($p, str_repeat('z', 100000));
    return [pclose($p), filesize($tmp)];
});
check('proc_close', function () {
    $p = proc_open('exec 1>&-; sleep 0.3; exit 5', [1 => ['pipe', 'w']], $pipes);
    return proc_close($p);
});
// proc_close() with the child waiting for EOF on a stdin we never closed.
check('proc_close open stdin', function () {
    $p = proc_open('sleep 0.3; cat > /dev/null; exit 6', [0 => ['pipe', 'r']], $pipes);
    return proc_close($p);
});
check('proc_get_status first', function () {
    $p = proc_open('exit 8', [], $pipes);
    usleep(300000);
    $s = proc_get_status($p);
    return [$s['running'], proc_close($p)];
});
unlink($tmp);
?>
--EXPECT--
shell_exec read        same cooperative
shell_exec close wait  same cooperative
exec                   same cooperative
system                 same cooperative
passthru               same cooperative
backticks              same cooperative
popen r + pclose       same cooperative
pclose unread output   same cooperative
popen w + pclose       same cooperative
proc_close             same cooperative
proc_close open stdin  same cooperative
proc_get_status first  same cooperative
