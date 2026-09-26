# phasync-ext

[![CI](https://github.com/phasync/phasync-ext/actions/workflows/ci.yml/badge.svg)](https://github.com/phasync/phasync-ext/actions/workflows/ci.yml)

A PHP extension that gives [phasync](https://github.com/phasync/phasync) — and any
fiber-based async code — two things on **PHP 8.2+**, without patching PHP:

1. **`phasync\ext\stream_select()`** — a drop-in `stream_select()` that uses
   `poll(2)` internally, so it is **not bounded by `FD_SETSIZE`** (the ~1024
   descriptor ceiling). Accepts stream resources *and* plain integer fds.

2. **Transparent async I/O via `phasync\ext\manage()`** — run a closure with
   blocking I/O cooperatively yielding the current fiber instead of blocking the
   process, for the dynamic extent of that closure. It covers:
   - `tcp://` / `unix://` sockets (incl. `fsockopen`, `stream_socket_client/server`),
     including `tcp://` connect + its DNS lookup, and `stream_socket_accept()`
   - `ssl://` / `tls://` sockets
   - `stream_select()` and `socket_select()`
   - `proc_open()` pipes and `proc_close()`; `popen()`/`pclose()`, `shell_exec()`
     (and backticks), `exec()`, `system()`, `passthru()` — both the output pipe
     and the wait for the child to exit
   - `sleep()`, `usleep()`, `time_nanosleep()`, `time_sleep_until()`
   - DNS: `gethostbyname()`, `gethostbynamel()`, `gethostbyaddr()`,
     `dns_get_record()`, `checkdnsrr()`/`dns_check_record()`, `getmxrr()`/`dns_get_mx()`
     (via a thread pool)
   - `fopen()` (regular files + FIFO open, via a thread pool)
   - filesystem metadata and namespace calls on network/FUSE mounts: `stat()`,
     `lstat()`, `file_exists()`, `is_file()`/`is_dir()`/`is_link()`/`is_readable()`/
     `is_writable()`/`is_executable()`, `filesize()`/`filemtime()` & co.,
     `realpath()`, `readlink()`, `linkinfo()`, `scandir()`, `glob()`,
     `opendir()`/`dir()`, `unlink()`, `rename()`, `mkdir()`, `rmdir()`
     (via a thread pool; see below)

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

The committed `phasync_arginfo.h` targets the latest PHP. On **PHP 8.2 and 8.3** the
`ZEND_RAW_FENTRY` macro has a different arity, so regenerate it first (needs no
network beyond the one-time PHP-Parser fetch `gen_stub` does itself):

```sh
phpize
php build/gen_stub.php -f phasync.stub.php   # regenerate arginfo for this PHP
./configure --enable-phasync && make
```

## Install

**Composer** — the package is a plain Composer library that bundles prebuilt
binaries (PHP 8.2–8.5 × x86_64/aarch64 × glibc/musl, carried on each release tag),
so `composer require phasync/phasync-ext` needs no compiler. A files-autoloaded
bootstrap can then activate it on the CLI with no `php.ini` edit and no root:

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

**PIE** isn't supported yet: PIE only installs packages of `type: php-ext`, and
this package is a library so that plain `composer require` works without a
compiler. Build from source (see Build) if you need a `php.ini`-managed install.

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
    \Closure $code,           // run with async I/O active; its return value is returned
    \Closure $readHandler,    // (resource $stream, ?float $timeout) — wait until readable
    \Closure $writeHandler,   // (resource $stream, ?float $timeout) — wait until writable
    \Closure $sleepHandler,   // (int $microseconds) — wait that long (a scheduler timer)
    string   $timeoutException, // class a handler throws when the wait's time ran out
): mixed;
```

The read/write handlers receive a real **PHP stream resource** — the socket/pipe
being read, or, for thread-pool ops (DNS/file/FIFO), a wrapper around the
worker's completion pipe, and for the wait for a child process (`pclose()`,
`proc_close()`, the end of `exec()` & co.) a wrapper around a pidfd that turns
readable when the child exits — so they can be handed straight to
`phasync::readable()`/`writable()` (or a native `stream_select()`) and ride the
event loop's single select. The handlers are responsible only for *waiting*; the
extension performs the actual I/O afterwards. A handler called outside an event
loop can just do a small blocking `phasync\ext\stream_select()` to satisfy the
contract. The sleep handler receives a µs duration.

The second handler argument is how long the **native** op would still wait, in
seconds: a socket's `stream_set_timeout()`/`default_socket_timeout`, counted per
low-level wait exactly as PHP counts it (a retry within the same wait gets only the
remaining time), or `null` where PHP would wait forever (pipes, files, FIFOs,
thread-pool operations). Handler return values are ignored: **returning means
"ready"**, and the op is retried. To report that the time ran out, the handler
**throws an instance of `$timeoutException`** (subclasses included). The extension
catches and clears it and finishes the op exactly as native PHP does on a socket
timeout — partial data or `false`, `stream_get_meta_data()['timed_out']` set, and
the notice PHP emits for a timed-out send — so no exception escapes. Any *other*
exception (such as a coroutine cancellation) propagates out of the I/O call
unchanged. This keeps `fgets()`/`fread()`/`fwrite()`/`stream_get_contents()`
identical to native PHP, including timeout behaviour, whether or not the extension
is loaded. phasync passes `phasync\TimeoutException::class` and its own
`phasync::readable()`/`writable()` as the handlers.

`manage()` scopes are **automatic and stacking**: the handlers apply only while
`$code` runs and are removed when it returns or throws (no separate enable/disable
step), and a nested `manage()` shadows the outer handlers and timeout class (LIFO),
restoring them on return. Sleep and the thread-pool ops only take effect inside a fiber; outside
one they run as the ordinary blocking call.

Descriptor-backed streams are **wrapped as soon as they are created**: sockets
from the tcp/unix/ssl transports (`stream_socket_client/server`, `fsockopen`),
`stream_socket_pair()`, `proc_open()` and `popen()` pipes, `fopen()` of regular files/FIFOs, and
the fd-backed `php://` streams (`php://stdin|stdout|stderr`, `php://fd/N`). Streams
that predate the hooks are swept up too — fds inherited across `ensure_loaded()`'s
re-exec are wrapped at request start, and the `STDIN`/`STDOUT`/`STDERR` constants
(which the CLI materialises lazily) are wrapped when the first `manage()` scope is
entered. A wrapped stream is
**indistinguishable from a raw one outside a `manage()` scope**: it blocks,
returns `EAGAIN`, and honours `stream_set_blocking()` exactly as an unwrapped
stream would. Only inside a scope, and only for a stream left in blocking mode,
does a would-block suspend the fiber instead of blocking the process.

Two coroutines waiting on the same stream in the same direction is the caller's
data race, as in Go or Rust; the extension does not serialise them (phasync refuses
a second waiter loudly).

```php
use function phasync\ext\manage;

manage(
    function () {
        // start fibers, drive them with phasync\ext\stream_select(); blocking
        // fread()/fwrite()/gethostbyname()/sleep() inside a fiber yield here.
    },
    // readable/writable: return once ready; throw MyTimeout if $timeout ran out
    fn($stream, ?float $timeout) => \Fiber::suspend($stream),
    fn($stream, ?float $timeout) => \Fiber::suspend($stream),
    fn(int $us) => \Fiber::suspend($us),      // timer
    MyTimeout::class,
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

Filesystem metadata calls (`stat()`, `file_exists()`, `scandir()`, `unlink()`, …)
take about a microsecond on a local disk, and a pool round trip would make them
slower (tens of µs), which hurts code that makes thousands of them, like an
autoloader. So by default only paths on network or FUSE mounts (NFS, SMB/CIFS, 9p,
CephFS, virtiofs, sshfs and other `fuse.*` …, read from `/proc/self/mountinfo`) go
through the pool, where a call can stall for a network round trip or longer. The
`phasync.fs_offload` INI changes that: `network` (default), `all` (also for slow
local disks), or `none`. Read-only calls stat or read the path on the pool, warming
the kernel's caches, and then run the original function, so results, PHP's stat
cache and warnings are exactly native. `unlink()`/`rename()`/`mkdir()`/`rmdir()`
run on the pool; if one fails, the original function runs and reports the error
with its usual warning.

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
