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
 * Run $code with transparent fiber-async I/O active for its dynamic extent.
 *
 * While $code runs, blocking I/O that would otherwise stall the process instead
 * calls one of the three handlers, which are responsible only for *waiting*
 * (typically Fiber::suspend into a scheduler); the extension performs the real
 * I/O afterwards:
 *   - $readHandler(resource $stream, ?float $timeout)  — wait until readable;
 *   - $writeHandler(resource $stream, ?float $timeout) — wait until writable;
 *   - $sleepHandler(int $us)                           — wait $us µs (a timer).
 *
 * The read/write handlers receive a real PHP stream resource — the socket/pipe
 * being read, or, for thread-pool ops (gethostbyname/file/FIFO), a wrapper around
 * the worker's completion pipe — so they can be phasync::readable()/writable()
 * (or feed a native stream_select()) directly, riding the loop's single select.
 *
 * $timeout is how many seconds the *native* op would still wait: a socket's
 * stream_set_timeout()/default_socket_timeout, counted per low-level wait as PHP
 * does (a retry within the same wait gets only the remaining time), or null where
 * PHP would wait forever (pipes, files, FIFOs, pool operations).
 *
 * Handler return values are ignored: returning means "ready", and the op is
 * retried. To report that the time ran out, a handler throws an instance of
 * $timeoutException (subclasses included); the extension catches and clears it
 * and finishes the op exactly as native PHP does on a socket timeout — partial
 * data or false, stream_get_meta_data()['timed_out'] set, and the notice PHP emits
 * for a timed-out send. Any other exception (e.g. a coroutine cancellation)
 * propagates out of the hooked function unchanged.
 *
 * Covers tcp/unix/ssl/tls sockets, stream_socket_pair(), proc_open() pipes,
 * STDIN/STDOUT/STDERR, sleep()/usleep()/time_nanosleep()/time_sleep_until(),
 * gethostbyname(), and fopen() (regular files and FIFO open() go through the
 * worker thread pool). A stream the caller made non-blocking is never waited on:
 * it behaves natively.
 *
 * Handlers are active only for the duration of this call and are removed
 * automatically when $code returns or throws — there is no separate on/off
 * step. Calls nest: an inner manage() shadows the outer handlers and timeout
 * class (LIFO) and the outer set is restored when it returns. Sleep and pool
 * handlers only take effect inside a fiber; outside one, the underlying call runs
 * normally (a real blocking op).
 *
 * Returns whatever $code returns.
 */
function manage(\Closure $code, \Closure $readHandler, \Closure $writeHandler, \Closure $sleepHandler, string $timeoutException): mixed {}
