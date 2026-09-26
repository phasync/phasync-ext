--TEST--
file_get_contents/file_put_contents/file/readfile/copy/md5_file read and write regular files via the pool inside a scope; include/require never do
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
?>
--FILE--
<?php
final class TestTimeout extends Exception {}
$pooled = 0;
$wait = function ($s, $t) use (&$pooled) {
    $pooled++;
    $r = [$s]; $w = $e = null;
    \phasync\ext\stream_select($r, $w, $e, 5);
};
$dir = sys_get_temp_dir() . '/phasync_ff_' . bin2hex(random_bytes(4));
mkdir($dir);
$data = str_repeat("line of text\n", 50000);   // 650 KB
file_put_contents("$dir/src", $data);

// Native first, then the same inside a scope, in a fiber: identical results, and
// the handler (the pool's completion wait) was called.
function check(string $name, Closure $f): void {
    global $wait, $pooled;
    ob_start(); $native = [$f(), ob_get_clean()];
    $before = $pooled; $ext = null;
    $code = function () use ($f, &$ext) { ob_start(); $ext = [$f(), ob_get_clean()]; };
    (new Fiber(fn() => \phasync\ext\manage($code, $wait, $wait, fn($us) => null, TestTimeout::class)))->start();
    printf("%-24s %s %s\n", $name, $native === $ext ? 'same' : 'DIFF', $pooled > $before ? 'pooled' : 'NOT POOLED');
}

check('file_get_contents',       fn() => md5(file_get_contents("$dir/src")));
check('file_get_contents range', fn() => file_get_contents("$dir/src", false, null, 13, 20));
check('file_put_contents',       fn() => [file_put_contents("$dir/out", $GLOBALS['data']), md5_file("$dir/out")]);
check('file_put_contents append', fn() => [file_put_contents("$dir/app", "x\n", FILE_APPEND), file_put_contents("$dir/app", "y\n", FILE_APPEND)]);
check('file',                    fn() => count(file("$dir/src")));
check('readfile',                fn() => strlen((string) readfile("$dir/src")));
check('copy',                    fn() => [copy("$dir/src", "$dir/copy"), filesize("$dir/copy")]);
check('md5_file',                fn() => md5_file("$dir/src"));
check('sha1_file',               fn() => sha1_file("$dir/src"));
check('hash_file',               fn() => hash_file('crc32b', "$dir/src"));
check('SplFileObject',           function () use ($dir) { $f = new SplFileObject("$dir/src"); $n = 0; foreach ($f as $l) $n++; return $n; });
check('stream_get_contents',     function () use ($dir) { $fp = fopen("$dir/src", 'r'); $r = md5(stream_get_contents($fp)); fclose($fp); return $r; });
// These mmap() plain files natively; inside a scope they read via the pool.
check('fpassthru',               function () use ($dir) { $fp = fopen("$dir/src", 'r'); $r = fpassthru($fp); fclose($fp); return $r; });
check('stream_copy_to_stream',   function () use ($dir) { $a = fopen("$dir/src", 'r'); $b = fopen("$dir/cp2", 'w'); $r = stream_copy_to_stream($a, $b); fclose($a); fclose($b); return [$r, md5_file("$dir/cp2")]; });

// include/require (and an autoloader's require) read through stdio streams too,
// but must never suspend mid-compile: not pooled, and they work.
file_put_contents("$dir/inc.php", '<?php return 42;');
file_put_contents("$dir/Cls.php", '<?php class Cls { const V = 7; }');
spl_autoload_register(function ($c) use ($dir) { require "$dir/$c.php"; });
$before = $pooled; $res = null;
$code = function () use ($dir, &$res) {
    ob_start(); highlight_file("$dir/inc.php"); $hl = ob_get_clean() !== '';   // compiles (tokenizes) too
    $res = [include "$dir/inc.php", require "$dir/inc.php", Cls::V, $hl];
};
(new Fiber(fn() => \phasync\ext\manage($code, $wait, $wait, fn($us) => null, TestTimeout::class)))->start();
var_dump($res, $pooled === $before);

// Outside a fiber: native, not pooled.
$before = $pooled;
\phasync\ext\manage(fn() => file_get_contents("$dir/src"), $wait, $wait, fn($us) => null, TestTimeout::class);
var_dump($pooled === $before);

foreach (scandir($dir) as $e) if ($e[0] !== '.') unlink("$dir/$e");
rmdir($dir);
?>
--EXPECT--
file_get_contents        same pooled
file_get_contents range  same pooled
file_put_contents        same pooled
file_put_contents append same pooled
file                     same pooled
readfile                 same pooled
copy                     same pooled
md5_file                 same pooled
sha1_file                same pooled
hash_file                same pooled
SplFileObject            same pooled
stream_get_contents      same pooled
fpassthru                same pooled
stream_copy_to_stream    same pooled
array(4) {
  [0]=>
  int(42)
  [1]=>
  int(42)
  [2]=>
  int(7)
  [3]=>
  bool(true)
}
bool(true)
bool(true)
