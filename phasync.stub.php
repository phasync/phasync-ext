<?php

/**
 * @generate-class-entries
 * @undocumentable
 */

namespace phasync\ext;

/**
 * Like the built-in stream_select(), but the descriptor set grows on demand,
 * so it is not bounded by FD_SETSIZE (the ~1024 ceiling) on any PHP version.
 * Array elements may be stream resources OR plain integer file descriptors.
 */
function stream_select(?array &$read, ?array &$write, ?array &$except, ?int $seconds, ?int $microseconds = null): int|false {}

/**
 * Waiting for streams, for an event loop, on epoll.
 *
 * The loop gives it its slots: getSlot(): int returns a slot number no one else
 * uses; park(int $slot, float $timeout): void suspends the current coroutine in
 * the slot until unpark(), or until $timeout seconds pass (then it throws the
 * loop's timeout exception) or it is cancelled (then it throws that);
 * unpark(int $slot): bool resumes the coroutine parked in the slot, false if the
 * slot is vacant (its wait was cancelled or timed out).
 *
 * A wait takes a fresh slot and parks in it; the Poller unparks only on PHP's
 * thread, inside poll(). poll() also wakes the waiters of hooked operations
 * started under a manage() that was given this Poller (thread-pool work such as
 * file operations and DNS). The loop keeps its Poller alive: waiters parked
 * through a Poller that is freed are never unparked, so their waits run to their
 * timeouts. A Poller belongs to the process that created it: after fork(),
 * create a new one (using this one throws).
 *
 * @strict-properties
 * @not-serializable
 */
final class Poller
{
    public function __construct(\Closure $getSlot, \Closure $park, \Closure $unpark) {}

    /**
     * Wait up to $maxTime seconds (0: don't wait) until something waited on is
     * ready, unpark its waiters, and return: the loop's one blocking call.
     */
    public function poll(float $maxTime): void {}

    /**
     * Park the current coroutine until $stream is readable, or at its end, or
     * failed (the next read then reports it the way PHP does). The loop's
     * exceptions (a timeout after $timeout seconds, a cancellation) propagate as
     * they are. One coroutine at a time may wait to read a stream (LogicException).
     *
     * @param resource $stream
     */
    public function readable(mixed $stream, float $timeout = \PHP_FLOAT_MAX): void {}

    /**
     * Park the current coroutine until $stream is writable, or closed, or failed,
     * as readable() does.
     *
     * @param resource $stream
     */
    public function writable(mixed $stream, float $timeout = \PHP_FLOAT_MAX): void {}
}

/**
 * Run $task with transparent fiber-async I/O active for its dynamic extent.
 *
 * While $task runs, blocking I/O inside a coroutine (fiber) parks it through
 * $poller instead of blocking the process, and the extension does the real I/O
 * once it may continue; the loop's $poller->poll() wakes it. $sleep(int
 * $microseconds) waits that long (a timer).
 *
 * When PHP's own call has a timeout (a socket's stream_set_timeout() or
 * default_socket_timeout), the extension passes it to park(); if park() throws
 * $timeoutException once it has run out, the call finishes exactly as native PHP
 * does on a timeout (partial data or false, stream_get_meta_data()['timed_out'],
 * the notice for a timed-out send). Any other exception (a cancellation)
 * propagates out of the hooked function unchanged.
 *
 * Covers tcp/unix/ssl/tls sockets, stream_socket_pair(), proc_open()/popen()
 * pipes and process waits, STDIN/STDOUT/STDERR and echo in the CLI,
 * stream_select()/socket_select(), the sleep functions, DNS, file and
 * filesystem calls (through a worker thread pool) and flock(). A stream the
 * caller made non-blocking is never waited on: it behaves natively.
 *
 * Calls nest: an inner manage() sends hooked I/O to its own Poller until it
 * returns. Outside a coroutine, calls behave natively. Returns what $task returns.
 */
function manage(\Closure $task, Poller $poller, \Closure $sleep, string $timeoutException): mixed {}
