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
 *   - $readHandler(int $fd)  — wait until $fd is readable;
 *   - $writeHandler(int $fd) — wait until $fd is writable;
 *   - $sleepHandler(int $us) — wait $us microseconds (a scheduler timer).
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
 * EXPERIMENTAL. Swap the global symbol table (the $GLOBALS / `global` table,
 * EG(symbol_table)) — a step toward per-coroutine global isolation.
 *
 * Installs the given context as the active global scope and returns an opaque
 * handle to the previously-installed one. Pass 0 to install a fresh empty scope.
 * Pass a handle returned earlier to restore that scope. The handle you get back
 * is "consumed" once you pass it in again; free unused handles with
 * free_symbols().
 *
 *   $saved = swap_symbols(0);   // isolate: fresh empty globals, keep the old
 *   // ... code here sees its own $GLOBALS ...
 *   $mine  = swap_symbols($saved);  // restore the caller's globals
 *   free_symbols($mine);            // discard the isolated scope
 *
 * Caveats (why this is experimental): it swaps ONLY globals, not function/class
 * statics; superglobals ($_SERVER, $_GET, …) are entries in the table, so a
 * fresh scope does not have them; and it must not run while a `global` binding
 * (an IS_INDIRECT into the outgoing table) is live — swap at coroutine
 * boundaries, and restore before the request ends.
 */
function swap_symbols(int $context = 0): int {}

/**
 * EXPERIMENTAL. Destroy a symbol-table context handle returned by
 * swap_symbols() that you no longer need. Never pass the currently-installed
 * scope's handle.
 */
function free_symbols(int $context): void {}
