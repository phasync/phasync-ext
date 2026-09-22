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
 * Register (or clear, with null) the callback invoked when a hooked stream read
 * would block. It receives the integer file descriptor and is responsible for
 * waiting until the fd is readable (typically Fiber::suspend into a scheduler).
 * The extension performs the actual read; the callback only waits.
 */
function register_read_handler(?callable $handler): void {}

/**
 * As register_read_handler(), but for writes that would block.
 */
function register_write_handler(?callable $handler): void {}

/**
 * Register (or clear, with null) the callback invoked by the hooked sleep()/
 * usleep(). It receives the duration in microseconds and is responsible for
 * waiting that long (typically Fiber::suspend with a timer into a scheduler).
 */
function register_sleep_handler(?callable $handler): void {}

/**
 * Install the hooks: re-register tcp:// and unix:// transports and override
 * proc_open(), sleep() and usleep(). Sockets and proc_open pipes created
 * afterwards route their would-block I/O through the registered handlers.
 * Idempotent.
 */
function enable_hooks(): void {}

/**
 * Remove all hooks, restoring the original transports and functions.
 */
function disable_hooks(): void {}
