<?php

/**
 * @generate-class-entries
 * @undocumentable
 */

namespace phasync;

/**
 * Like the built-in stream_select(), but the descriptor set grows on demand,
 * so it is not bounded by FD_SETSIZE (the ~1024 ceiling) on any PHP version.
 * Array elements may be stream resources OR plain integer file descriptors.
 */
function stream_select(?array &$read, ?array &$write, ?array &$except, ?int $seconds, ?int $microseconds = null): int|false {}

/**
 * Register (or clear, with null) the callback invoked when a hooked socket read
 * would block. It is called with the integer file descriptor and is responsible
 * for waiting until the fd is readable (typically Fiber::suspend into a scheduler).
 * The extension performs the actual read; the callback only waits.
 */
function register_read_handler(?callable $handler): void {}

/**
 * Register (or clear, with null) the callback invoked when a hooked socket write
 * would block. Called with the integer file descriptor; responsible for waiting
 * until the fd is writable.
 */
function register_write_handler(?callable $handler): void {}

/**
 * Install the transport hooks (re-register tcp:// and unix:// so sockets created
 * afterwards route their reads/writes through the registered handlers). Idempotent.
 */
function enable_hooks(): void {}

/**
 * Remove the transport hooks, restoring the original tcp:// and unix:// factories.
 */
function disable_hooks(): void {}
