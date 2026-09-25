--TEST--
phasync\ext\stream_select() matches native stream_select(): return count, µs timeouts, bad-fd error
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair'); ?>
--FILE--
<?php
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
fwrite($b, 'x');                                   // $a: readable AND writable

// 1. Count: select() counts set bits across all arrays, so $a counts twice.
foreach (['stream_select', 'phasync\ext\stream_select'] as $fn) {
    $r = [$a]; $w = [$a]; $e = null;
    printf("%s: %d r=%d w=%d\n", $fn === 'stream_select' ? 'native' : 'ext   ',
        $fn($r, $w, $e, 0), count($r), count($w));
}

// 2. A sub-millisecond timeout is honoured (it used to become a zero-time poll).
fread($a, 1);                                      // idle again
$r = [$a]; $w = $e = null;
$t = hrtime(true);
var_dump(\phasync\ext\stream_select($r, $w, $e, 0, 800));
$ms = (hrtime(true) - $t) / 1e6;
var_dump($ms >= 0.7 && $ms < 50);                  // waited ~0.8 ms

// 3. A bad descriptor fails like select()'s EBADF instead of reporting an event.
$r = [999999]; $w = $e = null;
var_dump(\phasync\ext\stream_select($r, $w, $e, 0));
fclose($a); fclose($b);
?>
--EXPECTF--
native: 2 r=1 w=1
ext   : 2 r=1 w=1
int(0)
bool(true)

Warning: phasync\ext\stream_select(): Unable to select [9]: Bad file descriptor (max_fd=999999) in %s on line %d
bool(false)
