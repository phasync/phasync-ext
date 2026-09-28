--TEST--
phasync.fs_offload=network (default): regular files on a local disk read and write inline, in and outside coroutines; =all offloads them, =none never
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
$tmp = realpath(sys_get_temp_dir());
foreach (file('/proc/self/mountinfo') as $l) {
    [$pre, $post] = explode(' - ', $l, 2);
    $type = strtok($post, ' ');
    $mnt = stripcslashes(explode(' ', $pre)[4]);
    if ((str_starts_with($type, 'fuse') || in_array($type, ['nfs', 'nfs4', 'cifs', 'smb3', '9p', 'virtiofs'], true))
        && ($mnt === '/' || str_starts_with("$tmp/", "$mnt/"))) {
        die('skip the temp dir is on a network/FUSE mount');
    }
}
?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
$loop = new Loop;
$dir = sys_get_temp_dir() . '/phasync_inline_' . bin2hex(random_bytes(4));
mkdir($dir);
// Three 8 KiB chunks, so fread() has to fetch from the fd each time.
$data = str_repeat('A', 8192) . str_repeat('B', 8192) . str_repeat('C', 8192);
file_put_contents("$dir/src", $data);

$work = function () use ($dir, $data) {
    $fp = fopen("$dir/src", 'r');
    $read = '';
    while (!feof($fp)) {
        $read .= fread($fp, 8192);
    }
    fclose($fp);
    ob_start(); readfile("$dir/src"); $rf = ob_get_clean();
    return [
        $read === $data,
        file_get_contents("$dir/src") === $data,
        file_get_contents("$dir/src", false, null, 8190, 4) === 'AABB',
        file_put_contents("$dir/out", $data) === strlen($data) && md5_file("$dir/out") === md5($data),
        $rf === $data,
        copy("$dir/src", "$dir/copy") && file_get_contents("$dir/copy") === $data,
    ];
};

foreach (['network', 'all', 'none'] as $policy) {
    ini_set('phasync.fs_offload', $policy);
    $before = $loop->parks;
    $in = null;
    $loop->runAll(function () use ($work, &$in) { $in = $work(); });
    printf("%-8s coroutine %s, %s\n", $policy, $in === array_fill(0, 6, true) ? 'correct' : 'WRONG', $loop->parks > $before ? 'pooled' : 'inline');
    $before = $loop->parks;
    $out = $loop->manage($work);
    printf("%-8s no coroutine %s, %s\n", $policy, $out === array_fill(0, 6, true) ? 'correct' : 'WRONG', $loop->parks > $before ? 'pooled' : 'inline');
}

// A stream found to read inline goes to the pool once the policy says so.
ini_set('phasync.fs_offload', 'network');
$loop->runAll(function () use ($dir, $loop) {
    $fp = fopen("$dir/src", 'r');
    $parks = $loop->parks;
    $a = fread($fp, 8192);
    echo 'network read: ', $a === str_repeat('A', 8192) ? 'ok' : 'WRONG', $loop->parks > $parks ? ' pooled' : ' inline', "\n";
    ini_set('phasync.fs_offload', 'all');
    $b = fread($fp, 8192);
    echo 'all read: ', $b === str_repeat('B', 8192) ? 'ok' : 'WRONG', $loop->parks > $parks ? ' pooled' : ' inline', "\n";
    fclose($fp);
});

foreach (scandir($dir) as $e) if ($e[0] !== '.') unlink("$dir/$e");
rmdir($dir);
?>
--EXPECT--
network  coroutine correct, inline
network  no coroutine correct, inline
all      coroutine correct, pooled
all      no coroutine correct, inline
none     coroutine correct, inline
none     no coroutine correct, inline
network read: ok inline
all read: ok pooled
