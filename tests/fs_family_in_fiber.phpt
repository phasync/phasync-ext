--TEST--
Filesystem metadata/namespace functions go through the pool inside a scope (phasync.fs_offload=all) and keep native results, warnings and stat cache
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (PHP_OS_FAMILY !== 'Linux') die('skip Linux only');
?>
--INI--
phasync.fs_offload=all
--FILE--
<?php
final class TestTimeout extends Exception {}
$pooled = 0;
$wait = function ($s, $t) use (&$pooled) {
    $pooled++;
    $r = [$s]; $w = $e = null;
    \phasync\ext\stream_select($r, $w, $e, 5);
};

// Two identical fixture trees; each case runs in one of them, natively or in a
// fiber inside a scope, with paths relative to the tree (so warnings compare).
function fixture(): string {
    $root = sys_get_temp_dir() . '/phasync_fs_' . bin2hex(random_bytes(4));
    mkdir("$root/d/sub", 0777, true);
    file_put_contents("$root/f", 'hello');
    file_put_contents("$root/d/a.txt", 'a');
    file_put_contents("$root/d/b.txt", 'b');
    file_put_contents("$root/g", 'g');
    symlink('f', "$root/l");
    return $root;
}
function rmtree(string $p): void {
    if (is_link($p) || is_file($p)) { unlink($p); return; }
    if (!is_dir($p)) return;
    foreach (scandir($p) as $e) if ($e !== '.' && $e !== '..') rmtree("$p/$e");
    rmdir($p);
}
function call(Closure $f): string {
    error_clear_last();
    $r = @$f();
    $w = error_get_last()['message'] ?? '';
    // stat arrays of the two fixture trees differ in inode and (maybe) times only
    if (is_array($r) && isset($r['ino'])) unset($r['ino'], $r[1], $r['atime'], $r[8], $r['mtime'], $r[9], $r['ctime'], $r[10]);
    return json_encode($r) . ($w !== '' ? " + $w" : '');
}
function check(string $name, Closure $f, bool $quiet = false): void {
    global $wait, $pooled;
    $a = fixture(); $b = fixture();
    chdir($a); $native = call($f);
    chdir($b); $before = $pooled; $ext = null;
    $code = function () use ($f, &$ext) { $ext = call($f); };   // not inside fn(): &$ext
    (new Fiber(fn() => \phasync\ext\manage($code, $wait, $wait, fn($us) => null, TestTimeout::class)))->start();
    chdir('/');
    rmtree($a); rmtree($b);
    $line = sprintf("%-26s %s %s\n", $name, $native === $ext ? 'same' : "DIFF native=$native ext=$ext",
        $pooled > $before ? 'pooled' : 'NOT POOLED');
    if (!$quiet || !str_ends_with($line, " same pooled\n")) echo $line;   // quiet: failures only
}

foreach (['f', 'd', 'l', 'missing'] as $p) {
    foreach (['stat', 'lstat', 'file_exists', 'is_file', 'is_dir', 'is_link', 'is_readable',
              'is_writable', 'is_writeable', 'is_executable', 'filesize', 'filemtime',
              'filectime', 'fileperms', 'fileowner', 'filegroup', 'filetype', 'realpath'] as $fn) {
        // realpath() differs by fixture root: compare the basename.
        check("$fn($p)", fn() => $fn === 'realpath' && ($r = realpath($p)) !== false ? basename($r) : $fn($p), true);
    }
}
echo "stat family checked\n";
check('readlink(l)',            fn() => readlink('l'));
check('readlink(f)',            fn() => readlink('f'));
check('linkinfo(missing)',      fn() => linkinfo('missing'));
check('scandir(d)',             fn() => scandir('d'));
check('scandir(missing)',       fn() => scandir('missing'));
check('glob(d/*.txt)',          fn() => glob('d/*.txt'));
check('glob(abs)',              fn() => array_map('basename', glob(getcwd() . '/d/*')));
check('opendir/readdir',        function () { $h = opendir('d'); $e = []; while (($x = readdir($h)) !== false) $e[] = $x; closedir($h); sort($e); return $e; });
check('opendir(missing)',       fn() => opendir('missing'));
check('dir(d)',                 function () { $d = dir('d'); $e = []; while (($x = $d->read()) !== false) $e[] = $x; $d->close(); sort($e); return $e; });
// Mutations; the stat cache must be cleared like native (file_exists() primes it).
check('unlink',                 fn() => [file_exists('g'), unlink('g'), file_exists('g')]);
check('unlink(missing)',        fn() => unlink('missing'));
check('unlink(dir)',            fn() => unlink('d'));
check('rmdir',                  fn() => [is_dir('d/sub'), rmdir('d/sub'), is_dir('d/sub')]);
check('rmdir(non-empty)',       fn() => rmdir('d'));
check('mkdir',                  fn() => [mkdir('new', 0750), decoct(fileperms('new') & 0777)]);
check('mkdir(existing)',        fn() => mkdir('d'));
check('mkdir(missing parent)',  fn() => mkdir('x/y'));
check('mkdir recursive',        fn() => [mkdir('p/q/r/', 0777, true), is_dir('p/q/r')]);
check('mkdir recursive exists', fn() => mkdir('d/sub', 0777, true));
check('mkdir recursive part',   fn() => [mkdir('d/sub/x/y', 0777, true), is_dir('d/sub/x/y')]);
check('mkdir recursive file',   fn() => mkdir('f/x', 0777, true));
check('rename',                 fn() => [file_exists('g'), rename('g', 'h'), file_exists('g'), file_get_contents('h')]);
check('rename(missing)',        fn() => rename('missing', 'h'));
check('rename dir over file',   fn() => rename('d', 'f'));

// Outside a fiber everything is native (and not pooled).
$before = $pooled;
\phasync\ext\manage(fn() => file_exists(__FILE__), $wait, $wait, fn($us) => null, TestTimeout::class);
var_dump($pooled === $before);

// The default policy (network) leaves a local path alone.
ini_set('phasync.fs_offload', 'network');
$root = fixture();
(new Fiber(fn() => \phasync\ext\manage(function () use ($root) {
    stat("$root/f"); scandir($root); unlink("$root/g");
}, $wait, $wait, fn($us) => null, TestTimeout::class)))->start();
var_dump($pooled === $before);
rmtree($root);
var_dump(ini_set('phasync.fs_offload', 'bogus'), ini_get('phasync.fs_offload'));
?>
--EXPECT--
stat family checked
readlink(l)                same pooled
readlink(f)                same pooled
linkinfo(missing)          same pooled
scandir(d)                 same pooled
scandir(missing)           same pooled
glob(d/*.txt)              same pooled
glob(abs)                  same pooled
opendir/readdir            same pooled
opendir(missing)           same pooled
dir(d)                     same pooled
unlink                     same pooled
unlink(missing)            same pooled
unlink(dir)                same pooled
rmdir                      same pooled
rmdir(non-empty)           same pooled
mkdir                      same pooled
mkdir(existing)            same pooled
mkdir(missing parent)      same pooled
mkdir recursive            same pooled
mkdir recursive exists     same pooled
mkdir recursive part       same pooled
mkdir recursive file       same pooled
rename                     same pooled
rename(missing)            same pooled
rename dir over file       same pooled
bool(true)
bool(true)
bool(false)
string(7) "network"
