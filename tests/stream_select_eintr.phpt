--TEST--
phasync\ext\stream_select() returns false with a warning when interrupted by a signal, like native
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!function_exists('pcntl_alarm') || !function_exists('pcntl_async_signals')) die('skip requires pcntl');
if (!function_exists('stream_socket_pair')) die('skip requires stream_socket_pair');
?>
--FILE--
<?php
// Native stream_select() does not retry after EINTR: it returns false with a
// warning. Retrying would also silently restart the whole timeout.
pcntl_async_signals(true);
pcntl_signal(SIGALRM, fn() => null);
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
foreach (['stream_select', 'phasync\ext\stream_select'] as $fn) {
    pcntl_alarm(1);
    $r = [$a]; $w = $e = null;
    $t = microtime(true);
    var_dump(@$fn($r, $w, $e, 5));
    echo preg_replace('/^.*?\(\): /', '', error_get_last()['message']), "\n";
    var_dump(microtime(true) - $t < 3);            // interrupted, not a full 5 s wait
}
?>
--EXPECTF--
bool(false)
Unable to select [4]: Interrupted system call (max_fd=%d)
bool(true)
bool(false)
Unable to select [4]: Interrupted system call (max_fd=%d)
bool(true)
