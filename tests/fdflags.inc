<?php
// Helpers for asserting a descriptor's REAL O_NONBLOCK state (not PHP's own
// belief, which stream_get_meta_data()['blocked'] reports) is left exactly as
// found by every cooperative operation (#32).

/** The fd's real O_NONBLOCK flag, read straight from the kernel via
 *  /proc/self/fdinfo, independent of anything phasync (or PHP) tracks. */
function real_nonblock(int $fd): bool
{
    $info = file_get_contents("/proc/self/fdinfo/$fd");
    if ($info === false || !preg_match('/^flags:\s*(\d+)$/m', $info, $m)) {
        throw new RuntimeException("no flags: line in fdinfo for fd $fd");
    }
    return (octdec($m[1]) & 04000) !== 0;   // O_NONBLOCK
}

/** $stream's real fd number, found by matching its inode (fstat) against
 *  /proc/self/fd/<n> -> "pipe:[N]" / "socket:[N]", one readlink(2) per
 *  candidate fd (no directory listing, which would self-reference its own
 *  transient fd in /proc/self/fd). */
function fd_for_stream($stream, int $max = 256): int
{
    $ino = fstat($stream)['ino'];
    for ($fd = 0; $fd < $max; $fd++) {
        $link = @readlink("/proc/self/fd/$fd");
        if ($link !== false && preg_match('/\[(\d+)\]/', $link, $m) && (int) $m[1] === $ino) {
            return $fd;
        }
    }
    throw new RuntimeException("fd for stream (ino $ino) not found below $max");
}
