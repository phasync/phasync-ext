--TEST--
A blocking socket pair's real O_NONBLOCK flag is unchanged before, during a park, and
after a cooperative read that must wait (#32)
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

var_dump(real_nonblock($fa), real_nonblock($fb));   // both blocking

$loop = new Loop;
$during = null;
$loop->runAll(
    function () use ($b) {
        echo "read: " . fread($b, 5) . "\n";   // nothing written yet: parks
    },
    function () use ($a, $fb, &$during) {
        $during = real_nonblock($fb);           // while the reader is parked
        fwrite($a, 'hello');
    },
);
var_dump($during);                              // unchanged while parked
var_dump(real_nonblock($fa), real_nonblock($fb)); // unchanged after

fclose($a);
fclose($b);
?>
--EXPECT--
bool(false)
bool(false)
read: hello
bool(false)
bool(false)
bool(false)
