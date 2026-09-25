# phasync-ext

[![CI](https://github.com/phasync/phasync-ext/actions/workflows/ci.yml/badge.svg)](https://github.com/phasync/phasync-ext/actions/workflows/ci.yml)

A PHP extension that gives [phasync](https://github.com/phasync/phasync) — and any
fiber-based async code — two things on **PHP 8.3+**, without patching PHP:

1. **`phasync\ext\stream_select()`** — a drop-in `stream_select()` that uses
   `poll(2)` internally, so it is **not bounded by `FD_SETSIZE`** (the ~1024
   descriptor ceiling). Accepts stream resources *and* plain integer fds.

2. **Transparent async I/O via `phasync\ext\manage()`** — run a closure with
   blocking I/O cooperatively yielding the current fiber instead of blocking the
   process, for the dynamic extent of that closure. It covers:
   - `tcp://` / `unix://` sockets (incl. `fsockopen`, `stream_socket_client/server`)
   - `ssl://` / `tls://` sockets
   - `proc_open()` pipes
   - `sleep()`, `usleep()`, `time_nanosleep()`, `time_sleep_until()`
   - `gethostbyname()` and `fopen()` (regular files + FIFO open, via a thread pool)

   The extension performs the real I/O; on a would-block it invokes one of the
   handlers you pass to `manage()`, which decides how to wait (typically
   `Fiber::suspend()` into a scheduler). The C side never touches the Fiber API —
   your callback owns all suspension. This works because PHP fibers are stackful,
   so a suspend from inside `fread()`/`SSL_read()` unwinds and resumes correctly.

## Why

`stream_select()` fails (warning + `false`) once any watched descriptor exceeds
`FD_SETSIZE`, and PHP userland has no control over which fd number a stream gets —
so it is unreliable for programs juggling many streams. This lifts that limit
today, and adds the interception points a fiber scheduler needs to make
`mysqli`/PDO, `fread`/`fwrite`, `proc_open` and the sleep functions transparently
async. (See also php-src PR #14452, which lifts the `stream_select()` limit in
core itself.)

## Build

```sh
phpize
./configure --enable-phasync
make
make test
# load per-run in CLI:  php -d extension=modules/phasync.so your-app.php
```

The committed `phasync_arginfo.h` targets the latest PHP. On **PHP 8.3** the
`ZEND_RAW_FENTRY` macro has a different arity, so regenerate it first (needs no
network beyond the one-time PHP-Parser fetch `gen_stub` does itself):

```sh
phpize
php build/gen_stub.php -f phasync.stub.php   # regenerate arginfo for this PHP
./configure --enable-phasync && make
```

## Install

**PIE** (the PECL successor) builds it from source against your PHP:

```sh
pie install phasync/phasync-ext
```

**Composer** — the package is `type: php-ext`, and it also ships a
files-autoloaded bootstrap so a plain `composer require` can activate it on the
CLI with no `php.ini` edit and no root:

```php
require 'vendor/autoload.php';
phasync\ext\ensure_loaded();   // call once, first thing
```

A C extension can't load itself from its own not-yet-loaded code, so
`ensure_loaded()` (plain PHP) checks whether the extension is present and, if not,
**re-execs the current CLI process** with `-d extension=<matching .so>` — i.e. it
lands on the normal, ABI-safe MINIT load. It preserves the original command line
(via `/proc/self/cmdline`, so your own `-d` flags survive), guards against a
re-exec loop, and is a no-op when the extension is already loaded (PIE,
`extension=`, or a prior re-exec). Uses `pcntl_exec()`, falling back to FFI
`execv()`. CLI only; on other SAPIs add `extension=phasync` to `php.ini`.

Or the plain manual load, from anywhere on disk (absolute path — no
`extension_dir` needed for `extension=`):

```sh
php -d extension=/path/to/phasync.so your-app.php
```

## API

```php
namespace phasync\ext;

function stream_select(?array &$read, ?array &$write, ?array &$except,
                       ?int $seconds, ?int $microseconds = null): int|false;

function manage(
    \Closure $code,          // run with async I/O active; its return value is returned
    \Closure $readHandler,   // (resource $stream) — wait until readable
    \Closure $writeHandler,  // (resource $stream) — wait until writable
    \Closure $sleepHandler,  // (int $microseconds) — wait that long (a scheduler timer)
): mixed;

function is_managed(mixed $stream): bool;
```

The read/write handlers receive a real **PHP stream resource** — the socket/pipe
being read, or, for thread-pool ops (`gethostbyname()`/file/FIFO), a wrapper
around the worker's completion pipe — so they can be handed straight to
`phasync::readable()`/`writable()` (or a native `stream_select()`) and ride the
event loop's single select. The handlers are responsible only for *waiting*; the
extension performs the actual I/O afterwards. A handler called outside an event
loop can just do a small blocking `phasync\ext\stream_select()` to satisfy the
contract. The sleep handler receives a µs duration.

`manage()` scopes are **automatic and stacking**: the handlers apply only while
`$code` runs and are removed when it returns or throws (no separate enable/disable
step), and a nested `manage()` shadows the outer handlers (LIFO), restoring them
on return. Sleep and the thread-pool ops only take effect inside a fiber; outside
one they run as the ordinary blocking call.

Descriptor-backed streams are **wrapped as soon as they are created** (and any
that predate the extension — `STDIN`/`STDOUT`/`STDERR` and fds inherited across
`ensure_loaded()`'s re-exec — are wrapped at request start). A wrapped stream is
**indistinguishable from a raw one outside a `manage()` scope**: it blocks,
returns `EAGAIN`, and honours `stream_set_blocking()` exactly as an unwrapped
stream would. Only inside a scope, and only for a stream left in blocking mode,
does a would-block suspend the fiber instead of blocking the process.

`is_managed($stream)` reports whether reads/writes on `$stream` are intercepted
inside a scope (i.e. would suspend rather than block). It is a **pure ops-pointer
check — no syscall** — so a scheduler can cheaply skip an explicit readiness wait
and let the read suspend on its own. It returns `false` for non-descriptor
streams (`php://memory`, userspace wrappers, …) and for **listening server
sockets**, whose `accept()` is not intercepted.

```php
use function phasync\ext\manage;

manage(
    function () {
        // start fibers, drive them with phasync\ext\stream_select(); blocking
        // fread()/fwrite()/gethostbyname()/sleep() inside a fiber yield here.
    },
    fn($stream) => \Fiber::suspend($stream),  // readable
    fn($stream) => \Fiber::suspend($stream),  // writable
    fn(int $us) => \Fiber::suspend($us),      // timer
);
```

### Filesystem and DNS (thread pool)

Regular files and DNS lookups cannot be made non-blocking with readiness polling
(a disk fd is always "ready"; `getaddrinfo()` blocks). Like libuv/Node, these are
offloaded to a small worker thread pool: the worker runs only the raw syscall
(never the Zend engine, so it is safe in a non-ZTS build) and wakes the parked
fiber through a self-pipe that reuses the scope's read handler. Workers are pooled;
the pool size is the `phasync.thread_pool_size` INI (default 8; idle workers cost
only a lazily-paged stack, so a larger pool is cheap).

Named pipes (FIFOs) are the case cooperative scheduling *cannot* solve at all:
`open()` blocks in the kernel until the other end is opened, before any fd exists
to poll — so a single-threaded scheduler deadlocks unconditionally. Each FIFO
`open()` therefore runs on its own dedicated thread, letting a reader and a writer
coroutine rendezvous concurrently. After the open, a FIFO honours `O_NONBLOCK`, so
its reads/writes use the ordinary readiness path. If phasync times out or cancels
the coroutine, the extension `pthread_cancel`s the thread stuck in `open()`
(cancellation is scoped to that one syscall) and reaps it, so nothing leaks.

## Status

v1. Sockets, TLS, pipes, the sleep functions, DNS and filesystem/FIFO I/O are
implemented and tested. Known TLS limitations: read/write intent is approximated
(TLS renegotiation wanting the opposite direction could stall — the
`stream_socket_get_crypto_status()` API on 8.5+ would resolve it), the `ssl://`
handshake still runs synchronously, and `stream_socket_enable_crypto()` on an
already-open socket is not yet re-wrapped.

Linux only for now (uses `poll(2)`, `fcntl`, and POSIX threads).

## License

MIT — see [LICENSE](LICENSE).
