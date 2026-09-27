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
 * Run $task with transparent fiber-async I/O active for its dynamic extent.
 *
 * While $task runs, blocking I/O inside a coroutine (fiber) parks it in the
 * event loop instead of blocking the process, and the extension does the real
 * I/O once it may continue. The event loop supplies the waiting:
 *   - $getSlot(): int                        — a slot number no one else uses;
 *   - $park(int $slot, float $timeout): void — suspend the current coroutine in
 *     the slot until unpark(), or until $timeout seconds pass (then it throws
 *     $timeoutException) or it is cancelled (then it throws that);
 *   - $unpark(int $slot): bool               — resume the coroutine parked in
 *     the slot; false if the slot is vacant (its wait was cancelled or timed out);
 *   - $sleep(int $microseconds): void        — wait that long (a timer).
 *
 * The extension takes a fresh slot for every wait and unparks only on PHP's
 * thread, inside poll(): I/O readiness and finished thread-pool tasks both wake
 * their waiters there, so poll() is the loop's one blocking call.
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
 * Calls nest: an inner manage() shadows the outer event loop until it returns.
 * Outside a coroutine, calls behave natively. Returns what $task returns.
 */
function manage(\Closure $task, \Closure $getSlot, \Closure $park, \Closure $unpark, \Closure $sleep, string $timeoutException): mixed {}

/**
 * The event loop's one blocking call: wait up to $maxTime seconds (0: don't wait)
 * for anything waited on to become ready, unpark those waiters and the waiters of
 * finished thread-pool tasks, and return. Must be called inside manage().
 */
function poll(float $maxTime): void {}

/**
 * Park the current coroutine until $stream is readable, or at its end, or
 * failed (the next read then reports it the way PHP does). The event loop's
 * exceptions (a timeout after $timeout seconds, a cancellation) propagate as
 * they are. One coroutine at a time may wait to read a stream (LogicException).
 *
 * @param resource $stream
 */
function readable($stream, ?float $timeout = null): void {}

/**
 * Park the current coroutine until $stream is writable, or closed, or failed,
 * as readable() does.
 *
 * @param resource $stream
 */
function writable($stream, ?float $timeout = null): void {}
