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
 *   - $readHandler(resource $stream)  — wait until $stream is readable;
 *   - $writeHandler(resource $stream) — wait until $stream is writable;
 *   - $sleepHandler(int $us)          — wait $us microseconds (a scheduler timer).
 *
 * The read/write handlers receive a real PHP stream resource — the socket/pipe
 * being read, or, for thread-pool ops (gethostbyname/file/FIFO), a wrapper around
 * the worker's completion pipe — so they can be phasync::readable()/writable()
 * (or feed a native stream_select()) directly, riding the loop's single select.
 *
 * Covers tcp/unix/ssl/tls sockets, proc_open() pipes, sleep()/usleep()/
 * time_nanosleep()/time_sleep_until(), gethostbyname(), and fopen() (regular
 * files and FIFO open() go through the worker thread pool).
 *
 * Handlers are active only for the duration of this call and are removed
 * automatically when $code returns or throws — there is no separate on/off
 * step. Calls nest: an inner manage() shadows the outer handlers (LIFO) and the
 * outer set is restored when it returns. Handlers only take effect inside a
 * fiber; outside one, the underlying call runs normally (a real blocking op).
 *
 * Returns whatever $code returns.
 */
function manage(\Closure $code, \Closure $readHandler, \Closure $writeHandler, \Closure $sleepHandler): mixed {}

/**
 * Whether a read or write on $stream *right now* would suspend the fiber instead
 * of blocking the process or returning EAGAIN — so a scheduler can skip an
 * explicit readiness wait and let the read suspend on its own.
 *
 * All of these must hold (each is a field check — no syscall):
 *   - a manage() scope is currently active;
 *   - $stream is a descriptor-backed stream wrapped by this extension;
 *   - it is not a listening server socket (accept() is not intercepted);
 *   - the caller has left it in blocking mode (an explicitly non-blocking stream
 *     returns EAGAIN rather than suspending).
 *
 * Returns false otherwise — including outside any scope, and for non-descriptor
 * streams (php://memory, userspace wrappers, …).
 */
function is_auto_managed(mixed $stream): bool {}
