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
     * poll(0) with nothing waited on and no finished thread task queued returns
     * without a syscall.
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

/**
 * Run $code as one request with its own output, response headers and request
 * body, so many requests can run concurrently in one worker.
 *
 * Inside, and in every fiber started from inside (a fiber belongs to the
 * boundary of the fiber that calls its start()), echo and the ob_*() functions,
 * header(), headers_list(), headers_sent(), http_response_code(), setcookie(),
 * header_register_callback(), register_shutdown_function(), set_error_handler(),
 * set_exception_handler(), the session functions, php://input,
 * request_parse_body(), connection_aborted() and ignore_user_abort() work as they
 * do in a request of their own (INI settings, such as session_name(), stay shared). What would go to the client goes to $sapi,
 * as a SAPI would receive it:
 *
 * - ub_write(string $data): bool (required): output leaving the output buffers.
 *   Return false if the client is gone: connection_aborted() becomes true, later
 *   output is discarded and, unless ignore_user_abort(true), the request ends
 *   as by exit().
 * - send_headers(int $status, ?string $statusLine, array $headers): void
 *   (required): once, before the first output or when the request ends without
 *   any. $headers are raw lines as headers_list() returns them; $statusLine is
 *   set by header('HTTP/1.1 ...').
 * - flush(): void: flush() was called.
 * - read_post(int $length): string: up to $length bytes of the request body,
 *   fewer only at its end. Without it the request has no body.
 * - request_info(): array: called once at the start: 'method', 'content_type'
 *   and 'content_length' of the request, as php://input and
 *   request_parse_body() need them.
 * - exit(int|string $status): void: exit() or die() was called inside. In a fiber
 *   other than the one running virtualize(), that fiber ends quietly, and the
 *   server should cancel the rest of the request.
 * - connection_aborted(): bool: asked by connection_aborted() and
 *   connection_status(), for a server that knows the client left.
 *
 * Methods other than the two required are used only if present. They run
 * outside the boundary (their own output goes where it would without
 * virtualize()) and may suspend.
 *
 * When $code returns or throws, the request ends as PHP ends one: shutdown
 * functions run, the output buffers are flushed (removable or not), and the
 * headers are sent if nothing sent them. An uncaught exception goes to the
 * request's exception handler if it set one, and otherwise propagates. An
 * exit() inside ends the request
 * without ending the worker; virtualize() then returns null. Fibers of the
 * request still running afterwards have their output discarded.
 * Global variables are not isolated. Nesting throws an Error.
 */
function virtualize(\Closure $code, object $sapi): mixed {}
