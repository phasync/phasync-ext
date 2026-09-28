--TEST--
phasync.fs_offload_types: regular-file reads and writes on the listed filesystem types go to the pool (metadata calls stay inline); unlisted, they don't
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
// The filesystem type of this directory's mount, matched by device (zfs on the
// development box, ext4 on CI).
$dir = __DIR__ . '/fs_offload_types_' . bin2hex(random_bytes(4));
mkdir($dir);
file_put_contents("$dir/f", $data = str_repeat('0123456789', 5000));
$dev = stat("$dir/f")['dev'];
$type = null;
foreach (file('/proc/self/mountinfo') as $l) {
    [$pre, $post] = explode(' - ', $l, 2);
    [$maj, $min] = explode(':', explode(' ', $pre)[2]);
    if ((($maj << 8) | ($min & 0xff) | (($min & ~0xff) << 12)) === $dev) $type = strtok($post, ' ');
}
$loop = new Loop;
$work = function () use ($dir, $data, $loop) {
    $p = $loop->parks;
    $meta = file_exists("$dir/f") && filesize("$dir/f") === strlen($data);
    $metaParks = $loop->parks - $p;
    $p = $loop->parks;
    $ok = file_get_contents("$dir/f") === $data
       && file_put_contents("$dir/g", $data) === strlen($data)
       && md5_file("$dir/g") === md5($data);
    $fp = fopen("$dir/f", 'r');
    fseek($fp, 49990);
    $ok = $ok && fread($fp, 100) === '0123456789' && ftell($fp) === 50000;
    fclose($fp);
    return sprintf('%s, reads %s, metadata %s', $ok && $meta ? 'correct' : 'WRONG',
        $loop->parks > $p ? 'pooled' : 'inline', $metaParks ? 'pooled' : 'inline');
};
foreach (["other, $type", 'nfs', '', "$type,"] as $types) {
    ini_set('phasync.fs_offload_types', $types);
    $out = null;
    $loop->runAll(function () use ($work, &$out) { $out = $work(); });
    printf("%-10s %s\n", str_replace((string) $type, 'TYPE', $types) === '' ? "''" : str_replace((string) $type, 'TYPE', $types), $out);
}
$out = $loop->manage($work);
echo "no coroutine: $out\n";
foreach (['f', 'g'] as $e) unlink("$dir/$e");
rmdir($dir);
?>
--EXPECT--
other, TYPE correct, reads pooled, metadata inline
nfs        correct, reads inline, metadata inline
''         correct, reads inline, metadata inline
TYPE,      correct, reads pooled, metadata inline
no coroutine: correct, reads inline, metadata inline
