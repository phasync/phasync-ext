--TEST--
An explicitly non-blocking socket pair is untouched: no cooperation, real O_NONBLOCK flag
stays set, EAGAIN passed straight back to PHP (#32)
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!is_dir('/proc/self/fd')) die('skip requires /proc'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
require __DIR__ . '/fdflags.inc';

[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
$fa = fd_for_stream($a);
$fb = fd_for_stream($b);
stream_set_blocking($b, false);

var_dump(real_nonblock($fa), real_nonblock($fb));  // false, true

$loop = new Loop;
$loop->runAll(function () use ($b) {
    var_dump(fread($b, 5));   // native non-blocking read on empty socket: '' at once
});
var_dump($loop->parks);        // 0: never cooperated
var_dump(real_nonblock($fa), real_nonblock($fb));  // unchanged

fclose($a);
fclose($b);
?>
--EXPECT--
bool(false)
bool(true)
string(0) ""
int(0)
bool(false)
bool(true)
