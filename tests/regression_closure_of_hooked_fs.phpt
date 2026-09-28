--TEST--
A closure of a hooked filesystem function (is_file(...), Closure::fromCallable(), array_map(realpath(...))) works in and out of a coroutine (#9)
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
require __DIR__ . '/loop.inc';

$dir = sys_get_temp_dir() . '/phasync_cl_' . bin2hex(random_bytes(4));
mkdir($dir);
touch("$dir/f");

function run(string $dir): array {
    $isFile = is_file(...);
    $exists = Closure::fromCallable('file_exists');
    $unlink = Closure::fromCallable('unlink');
    return [
        $isFile("$dir/f"),
        $isFile($dir),
        $exists("$dir/f"),
        array_map(realpath(...), ["$dir/f", "$dir/nope", "$dir/."]) === ["$dir/f", false, $dir],
        array_filter(array_map(is_dir(...), [$dir, "$dir/f"])),
        filesize(...)("$dir/f"),
        @$unlink("$dir/nope"),
    ];
}

echo 'outside: ', json_encode(run($dir)), "\n";
$in = null;
(new Loop)->runAll(function () use ($dir, &$in) { $in = run($dir); });
echo 'inside:  ', json_encode($in), "\n";

unlink("$dir/f");
rmdir($dir);
?>
--EXPECT--
outside: [true,false,true,true,[true],0,false]
inside:  [true,false,true,true,[true],0,false]
