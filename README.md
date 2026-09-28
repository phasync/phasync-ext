# phasync-ext

[![CI](https://github.com/phasync/phasync-ext/actions/workflows/ci.yml/badge.svg)](https://github.com/phasync/phasync-ext/actions/workflows/ci.yml)

**Make the PHP you already have cooperate.** Inside a [phasync](https://github.com/phasync/phasync)
coroutine, a MySQL query through PDO, a Guzzle request or a `file_get_contents()` call waits as a coroutine
instead of blocking the process, and returns exactly what PHP would have returned. No new client
libraries, no changes to your code:

- **Under PHP-FPM**, one request overlaps its database queries, API calls and file work.
- **Under [swerve](https://github.com/phasync/swerve)**, a worker keeps thousands of connections
  busy, waiting on them with epoll: at 50,000 connections a hello-world server handled 2 to 4
  times as many requests per second as with PHP's own `stream_select()`.

`composer require phasync/phasync-ext` installs prebuilt binaries; nothing to compile. MIT, no
dependencies: [the Ennerd philosophy](PHILOSOPHY.md).

---

A PHP extension that gives [phasync](https://github.com/phasync/phasync) — and any
fiber-based async code — two things on **PHP 8.2+**, without patching PHP:

1. **`phasync\ext\stream_select()`** — a drop-in `stream_select()` that uses
   `poll(2)` internally, so it is **not bounded by `FD_SETSIZE`** (the ~1024
   descriptor ceiling). Accepts stream resources *and* plain integer fds.

2. **Transparent async I/O via `phasync\ext\manage()`** — run a closure with
   blocking I/O cooperatively yielding the current fiber instead of blocking the
   process, for the dynamic extent of that closure. It covers:
   - `tcp://` / `unix://` / `udp://` / `udg://` sockets (incl. `fsockopen`,
     `stream_socket_client/server`), including `tcp://` connect + its DNS lookup,
     `stream_socket_accept()`, and `stream_socket_recvfrom()`/`sendto()`
   - `ssl://` / `tls://` sockets, including the TLS handshake (in `tls://` connect
     and accept, and `stream_socket_enable_crypto()` on a blocking stream, after
     which the stream's reads and writes go through TLS)
   - `stream_select()` and `socket_select()`
   - ext/sockets: `socket_read()`, `socket_recv()`, `socket_recvfrom()`,
     `socket_write()`, `socket_send()`, `socket_sendto()`, `socket_accept()`,
     `socket_connect()` (numeric addresses and unix paths), honouring
     `SO_RCVTIMEO`/`SO_SNDTIMEO` as native timeouts
   - `curl_multi_select()`, so curl-multi loops (Guzzle's multi handler) don't block
     (probed without waiting and slept on between probes, 1ms doubling to 20ms:
     PHP doesn't expose curl's sockets), and `curl_exec()`, run on a private
     curl_multi with native results and errors (callbacks run in the coroutine)
   - `sem_acquire()` and `msg_receive()` (their non-blocking forms retried from the
     loop, so a cancelled wait takes nothing)
   - `proc_open()` pipes and `proc_close()`; `popen()`/`pclose()`, `shell_exec()`
     (and backticks), `exec()`, `system()`, `passthru()` — both the output pipe
     and the wait for the child to exit; `pcntl_waitpid()`/`pcntl_wait()`
   - `STDIN`/`STDOUT`/`STDERR` and `php://stdin|stdout|stderr` reads and writes
     on pipes, sockets and ttys; in the CLI also `echo`/`print` to a stdout that
     would block (each echo stays contiguous, as natively)
   - `sleep()`, `usleep()`, `time_nanosleep()`, `time_sleep_until()`
   - `flock()`, `file_put_contents(..., LOCK_EX)`, `SplFileObject::flock()` on a
     held lock (retried without blocking, sleeping via the loop's sleep between
     tries: 1ms, doubling to 20ms — there is no descriptor to wait on)
   - DNS: `gethostbyname()`, `gethostbynamel()`, `gethostbyaddr()`,
     `dns_get_record()`, `checkdnsrr()`/`dns_check_record()`, `getmxrr()`/`dns_get_mx()`
     (via a thread pool)
   - regular-file reads and writes, via a thread pool: `fopen()`'d files (and FIFO
     open), `file_get_contents()`, `file_put_contents()`, `file()`, `readfile()`,
     `fpassthru()`, `copy()`, `stream_copy_to_stream()`, `md5_file()`/`sha1_file()`/
     `hash_file()`, `SplFileObject`, `fsync()`/`fdatasync()` — never
     `include`/`require`, which must not suspend mid-compile
   - filesystem metadata and namespace calls on network/FUSE mounts: `stat()`,
     `lstat()`, `file_exists()`, `is_file()`/`is_dir()`/`is_link()`/`is_readable()`/
     `is_writable()`/`is_executable()`, `filesize()`/`filemtime()` & co.,
     `realpath()`, `readlink()`, `linkinfo()`, `scandir()`, `glob()`,
     `opendir()`/`dir()`, `unlink()`, `rename()`, `mkdir()`, `rmdir()`, `touch()`,
     `chmod()`/`chown()`/`chgrp()` & `l*` variants, `link()`, `symlink()`,
     `tempnam()`, `disk_free_space()`/`disk_total_space()`
     (via a thread pool; see below)

   The extension performs the real I/O; on a would-block it parks the coroutine
   through the event loop's `phasync\ext\Poller` you pass to `manage()`, whose
   `poll()` — the loop's one blocking call, on epoll — unparks it when it may
   continue. A `Poller` also works on its own, without `manage()`, for a loop that
   only wants epoll-based waiting. The C side never touches the Fiber API: the loop owns all
   suspension. This works because PHP fibers are stackful, so a suspend from
   inside `fread()`/`SSL_read()` unwinds and resumes correctly.

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

final class Poller {
    public function __construct(\Closure $getSlot,  // (): int — a slot number no one else uses
                                \Closure $park,     // (int $slot, float $timeout) — suspend the coroutine in the slot
                                \Closure $unpark);  // (int $slot): bool — resume it; false if the slot is vacant
    public function poll(float $maxTime): void;     // the loop's one blocking call
    public function readable(mixed $stream, float $timeout = PHP_FLOAT_MAX): void;
    public function writable(mixed $stream, float $timeout = PHP_FLOAT_MAX): void;
}

function manage(
    \Closure $task,             // run with async I/O active; its return value is returned
    Poller   $poller,           // the loop's Poller: hooked I/O waits through it
    \Closure $sleep,            // (int $microseconds) — wait that long (a timer)
    string   $timeoutException, // what park() throws when its $timeout ran out
): mixed;
```

**Waiting.** A coroutine waits by parking in a slot of the event loop: the
`Poller` calls `park($slot, $timeout)` with a slot from `getSlot()`; the loop
suspends the coroutine and resumes it when `unpark($slot)` is called. The
`Poller` unparks **only on PHP's thread, inside `poll()`**. A stream keeps its
epoll registration, level-triggered and left armed after a wait, and one slot
per direction: in request/response I/O nothing arrives while the coroutine is
busy, so a wait costs no syscall besides the batched `epoll_wait()`. An event
that finds nobody waiting disarms its direction then and there, so unread data
with nobody waiting costs one wake-up, never a busy loop. Worker threads never
call PHP — the thread-pool work
of hooked operations (file operations, DNS) started under a `manage()` given this
`Poller` queues the slot and writes to an eventfd in its epoll set, which `poll()`
drains. So `poll($maxTime)` replaces the loop's `stream_select()` and idle
`usleep()`: one `epoll_wait()` for up to `$maxTime` seconds (0 when it has work
queued, else the time until its next timer), costing work per ready event, never
a scan of everything registered; `poll(0)` with nothing waited on and no finished
thread task queued returns without a syscall. A registration lives as long as its stream and is
removed in the stream's close op; a coroutine waiting on a stream that is closed is
woken, and its next use of the stream finds it closed.

The loop keeps its `Poller` alive: waiters parked through a `Poller` that is freed
are never unparked, so their waits run to their timeouts. A `Poller` belongs to the
process that created it: after `fork()`, create a new one (using the old one
throws). Its closures usually capture the loop that owns it; the cycle collector
handles that.

**Timeouts and cancellation are the loop's.** When PHP's own call has a timeout
(a socket's `stream_set_timeout()` or `default_socket_timeout`, counted per
low-level wait as PHP counts it — a retry gets only the remaining time), the
extension passes it to `park()`; where PHP would wait forever (pipes, files,
FIFOs, pool operations) it passes none (`PHP_FLOAT_MAX`). If `park()` throws
`$timeoutException` once that time has run out, the extension finishes the call
exactly as native PHP does on a timeout — partial data or `false`,
`stream_get_meta_data()['timed_out']`, the notice for a timed-out send — so no
exception escapes. Any other exception (a cancellation) propagates out of the I/O
call unchanged, after the extension has disarmed the wait. `Poller::readable()`
and `writable()` pass the loop's exceptions through as they are. phasync gives the
`Poller` its loop's `getSlot`/`park`/`unpark`, and `manage()` its sleep and
`phasync\TimeoutException::class`.

`manage()` scopes are **automatic and stacking**: they apply only while `$task`
runs, and a nested `manage()` sends hooked I/O to its own `Poller` until it
returns. Waits only
happen inside a coroutine (fiber); outside one, calls run as the ordinary
blocking call.

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
does a would-block park the coroutine instead of blocking the process.

One coroutine at a time may wait on a stream in each direction: a second waiter
gets a `LogicException`, as with phasync's own pollers.

A minimal loop on this contract is in [`tests/loop.inc`](tests/loop.inc).

### Filesystem and DNS (thread pool)

Regular files and DNS lookups cannot be made non-blocking with readiness polling
(a disk fd is always "ready"; `getaddrinfo()` blocks). Like libuv/Node, these are
offloaded to a small worker thread pool: the worker runs only the raw syscall
(never the Zend engine, so it is safe in a non-ZTS build) and queues the parked
coroutine's slot for `poll()` to unpark. Workers are pooled;
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

0.5 (alpha): the slot/`poll()` contract. Sockets, TLS, pipes, processes, the
sleep functions, DNS, files and filesystem calls, flock and stdio are implemented
and tested. Known TLS limitations: read/write intent is approximated (TLS
renegotiation wanting the opposite direction could stall — the
`stream_socket_get_crypto_status()` API on 8.5+ would resolve it), and the TCP
connect and DNS lookup inside a `tls://` connect still run synchronously (ext/openssl
calls the socket layer directly there); the handshake itself cooperates.

Linux only for now (uses epoll, eventfd, `poll(2)`, `fcntl`, and POSIX threads).

## License

MIT — see [LICENSE](LICENSE).
