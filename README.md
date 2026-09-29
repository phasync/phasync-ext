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
   - regular-file reads that would wait for the disk, and reads and writes on
     network/FUSE mounts, via a thread pool: `fopen()`'d files, `file_get_contents()`, `file_put_contents()`, `file()`, `readfile()`,
     `fpassthru()`, `copy()`, `stream_copy_to_stream()`, `md5_file()`/`sha1_file()`/
     `hash_file()`, `SplFileObject` — never `include`/`require`, which must not
     suspend mid-compile; and on any disk `fsync()`/`fdatasync()` and FIFO `open()`
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

function virtualize(
    \Closure $code,             // one request; its return value is returned
    object   $sapi,             // where its response goes: ub_write(), send_headers(), ...
): mixed;

function set_preempt_function(?\Closure $fn, float $minInterval = 0.1): ?\Closure;
```

```php
namespace phasync;

#[\Attribute(\Attribute::TARGET_FUNCTION | \Attribute::TARGET_METHOD)]
final class Uninterruptible {}
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
removed in the stream's close op. A coroutine suspended in an operation on a stream
(`fread()`, `stream_socket_accept()`, `flock()`, a mysqli query, a read on the thread
pool ...) that another coroutine closes has that operation fail as on a closed
descriptor (EBADF); the close waits in its coroutine until the operation has let go
of the stream. Closing it outside a coroutine meanwhile is a fatal error.
`socket_close()` likewise wakes a coroutine waiting on the `Socket`.

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

**The extension's safety doesn't depend on PHP code**, neither the application's
nor the loop's. Each coroutine has a ledger, in C, of what the extension holds
for it while it waits: its Poller registration and slot, its place among a
stream's operations in flight, a pool operation, a closer's wait. The code after
the suspension settles it; if the fiber never gets back there, its destroy
observer does. A pool operation owns its buffer and a duplicate of its
descriptor, so a coroutine destroyed meanwhile leaves it to its thread, which
frees it. A slot is taken back before its coroutine is gone, so the extension
never wakes a dead one, whatever the loop still records. A coroutine closing a
stream others are inside an operation on waits for them, and is kept alive
meanwhile. A coroutine destroyed while it waits (dropped, or torn down by phasync)
thus ends its operation safely; what the operation returns is moot, since the
fiber is unwinding. In its `finally` blocks a wait fails with the `FiberError`
PHP throws for `Fiber::suspend()` there; pool operations run inline.

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
local disks), or `none`. The mount table is checked for changes at most once a
second, so a new mount counts within a second and no filesystem call pays a
syscall for the check. Offloaded read-only calls stat or read the path on the
pool, warming the kernel's caches, and then run the original function, so
results, PHP's stat cache and warnings are exactly native.
`unlink()`/`rename()`/`mkdir()`/`rmdir()` run on the pool; if one fails, the
original function runs and reports the error with its usual warning.

Reads of regular files follow the same policy by the file's device, and on a
local disk they ask the kernel first: `preadv2()` with `RWF_NOWAIT` (Linux 4.14)
reads what is in the page cache, inline in one syscall, and says `EAGAIN` when it
would wait for the disk; only then does the read (or the rest of a partly cached
one) go to the pool. A cold read on a spinning disk then parks one coroutine for
the seek instead of stalling the worker. Some filesystems refuse `RWF_NOWAIT`,
ZFS among them (its ARC is not the page cache): their files read inline, as
natively, and the refusal is remembered per mount, so no read repeats it. Writes
stay inline on a local disk; a buffered write waits for the disk only under
writeback pressure.

`phasync.fs_offload_types` (comma-separated, default empty) lists filesystem types
whose regular-file reads and writes always go to the pool, as on a network mount:
`phasync.fs_offload_types=zfs` for ZFS on spinning disks. A cached read then costs
a pool round trip (tens of µs); metadata calls stay inline, since an autoloader
makes thousands and the kernel keeps them cached. It takes types rather than paths
because one type covers every mount of it (ZFS mounts each dataset on its own).

Named pipes (FIFOs) are the case cooperative scheduling *cannot* solve at all:
`open()` blocks in the kernel until the other end is opened, before any fd exists
to poll — so a single-threaded scheduler deadlocks unconditionally. Each FIFO
`open()` therefore runs on its own dedicated thread, letting a reader and a writer
coroutine rendezvous concurrently. After the open, a FIFO honours `O_NONBLOCK`, so
its reads/writes use the ordinary readiness path. If phasync times out or cancels
the coroutine, the extension `pthread_cancel`s the thread stuck in `open()`
(cancellation is scoped to that one syscall) and reaps it, so nothing leaks.

### One request per boundary: `virtualize()`

A server running many requests concurrently in one worker can let ordinary PHP
code use `echo`, `header()` and friends: `virtualize($code, $sapi)` runs `$code`
as a request of its own. It, and every fiber started from inside it (a fiber
belongs to the boundary of the fiber that calls its `start()`; phasync starts a
coroutine from the fiber creating it), get their own copy of the per-request
state PHP otherwise keeps once per process:

| Functions | State |
|---|---|
| `echo`, `print`, `ob_*()` | the output buffers |
| `header()`, `header_remove()`, `headers_list()`, `headers_sent()`, `http_response_code()`, `setcookie()`, session cookies | the response headers and status |
| `header_register_callback()` | the header callback |
| `php://input`, `request_parse_body()`, `is_uploaded_file()`, `move_uploaded_file()` | the request body and uploads |
| `filter_input()`, and the superglobals as the request starts | the request's input |
| `connection_aborted()`, `connection_status()`, `ignore_user_abort()` | user-abort state |
| `register_shutdown_function()` | the shutdown functions |
| `set_error_handler()`, `set_exception_handler()`, `restore_*()` | the error and exception handlers |
| `session_start()`, `session_id()`, `session_status()`, `session_set_save_handler()`, ... | the session: its id, status, save handler and data |

The fiber observers swap that state when execution moves between boundaries, so
these functions run PHP's own code, with no override and no cost outside a
boundary. What leaves the boundary goes to `$sapi` the way a SAPI receives it,
through PHP's SAPI callbacks:

| Method | |
|---|---|
| `ub_write(string $data): bool` | required: output leaving the buffers; `false` = the client is gone |
| `send_headers(int $status, ?string $statusLine, array $headers): void` | required: once, before the first output or when the request ends without any; raw lines as `headers_list()` gives them, with PHP's default `Content-type` |
| `flush(): void` | `flush()` was called |
| `read_post(int $length): string` | the request body: up to `$length` bytes, fewer only at its end |
| `request_info(): array` | once at the start: `method`, `content_type`, `content_length`, `query_string`, `request_uri` |
| `read_cookies(): ?string` | once at the start: the `Cookie` header |
| `register_server_variables(): array` | once at the start: `$_SERVER`'s entries (`REQUEST_METHOD`, `QUERY_STRING`, `HTTP_*`, `REMOTE_ADDR`, ...) |
| `exit(int\|string $status): void` | `exit()`/`die()` was called inside |
| `connection_aborted(): bool` | for a server that knows the client left before a write fails |

The object is duck-typed: the extension declares no PHP type, looks the methods
up by name and uses the optional ones only if present, so the extension and a
server can be upgraded independently. The methods run outside the boundary
(their own output and warnings go where they would without `virtualize()`) and
may suspend: a `ub_write()` writing to the client's socket parks the coroutine
through the `Poller` like any other write.

The request ends as PHP ends one when `$code` returns or throws: shutdown
functions run, the output buffers are flushed (removable or not), and the headers
are sent if no output sent them. An uncaught exception goes to the request's
exception handler if it set one, and otherwise propagates from `virtualize()`.
`exit()` inside ends the request, not the worker, and doesn't change the
worker's exit status: in the fiber running `virtualize()`, `virtualize()`
returns null; in a fiber started inside, that fiber ends quietly and
`$sapi->exit()` lets the server cancel the rest. A client that is gone
(`ub_write()` returning `false`) aborts the request as PHP does: output stops,
and unless `ignore_user_abort(true)`, the request ends as by `exit()`. Fibers
of a request still running after it ended have their output discarded.

The superglobals are built at the start of the request by PHP's own code, as
PHP builds them from a SAPI: `$_GET` from the query string and `$_COOKIE` from
the Cookie header by PHP's parser, `$_SERVER` from `register_server_variables()`
plus PHP's `REQUEST_TIME` entries (no `argv`, and not the process environment),
`$_POST` and `$_FILES` from a POST's urlencoded or multipart body (read through
`read_post()` before the code runs, as PHP does; `move_uploaded_file()` works),
and `$_REQUEST` by `request_order`. Without the optional methods they are empty
arrays. Other bodies are read only when the code reads `php://input`.

Each request starts with the session state of a fresh request (no session, the
worker's save handler) and ends by writing and closing its session, as PHP ends a
request. Two concurrent requests on the same session take turns on its lock, as
under php-fpm: with the files handler, inside `manage()`, the second waits
cooperatively instead of blocking the worker. Settings behind INI entries, such
as `session_name()` and the cookie parameters, stay shared by the worker's
requests, like any `ini_set()`.

Global variables are not isolated, the superglobals and `$_SESSION` included:
PHP builds the request's superglobals into them as it starts, and
`session_start()` sets `$_SESSION`. A server running requests concurrently swaps
them itself around each resume of a request's fibers:

```php
final class RequestGlobals
{
    private array $own = [], $outer = [];

    public function enter(): void   // before the request's code runs or resumes
    {
        $this->outer = [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST, &$_SESSION];
        unset($_SESSION);
        if ($this->own) {
            [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST] = $this->own;
            if (isset($this->own[6])) $_SESSION = &$this->own[6];
        }
    }

    public function leave(): void   // once it suspends or ends
    {
        $this->own = [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST, &$_SESSION];
        unset($_SESSION);
        [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST] = $this->outer;
        if (isset($this->outer[6])) $_SESSION = &$this->outer[6];
    }
}
```

`$_SESSION` is rebound by reference, so it stays the reference ext/session saves
from; the other arrays by value, as some of PHP's C code (`SoapServer`, the URL
rewriter) reads `$_SERVER` without dereferencing a reference. Any other state a
server keeps per request (static properties, `date_default_timezone_set()`, ...)
swaps the same way.

A fatal error still ends the worker (after being displayed in the request's
output, as natively). Nesting `virtualize()` throws.

### Preemption: `set_preempt_function()`

`set_preempt_function($fn, $minInterval)` calls `$fn` at most every
`$minInterval` seconds (wall clock), between iterations of whatever PHP loop is
running, so a scheduler can preempt a coroutine that never yields: `$fn` may
call `Fiber::suspend()`, and resuming the fiber continues the loop.

Preemption happens only between loop iterations, in PHP code called from PHP
code: never in callbacks called by C functions (`usort()`, `array_map()`, output
handlers, session handlers, stream wrappers ...), in code the engine calls in
the middle of an operation (error handlers, magic methods, `__toString()`,
`Iterator` methods driven by `foreach`, autoloaders), in destructors, in
exception handlers and shutdown functions, or in `#[\phasync\Uninterruptible]`
functions and what they call. The stack is checked down to the fiber's first
frame (outside fibers, to the script's own code).

- Every backward jump (a loop's back-edge, a `foreach` `continue`, a backward
  `goto`) is compiled with a checkpoint before it:
  `if (\phasync\ext\__PREEMPT_DUE) \phasync\ext\checkpoint();`. Both are
  internal: the constant is a flag the extension flips (read-only to PHP code,
  not for use), and the function is in no function table on PHP 8.4+ (a
  frameless call; on 8.2/8.3 `function_exists()` sees it). The constant is
  registered per request, so opcache never folds it, and both JITs compile its
  read into loads and a compare, without a call; the call runs only when due
  and shows in no backtrace. A timer thread, not a signal, raises the flag, so
  no syscall fails with `EINTR` and the engine's interrupts are not used. `$fn`
  runs once, without arguments, at the next checkpoint the stack allows, the same in the
  interpreter and both JIT modes. A backward `goto` counts as a loop: the
  attribute is the way to make such code uninterruptible. Code without a loop
  (straight-line code, recursion) is never interrupted, nor is code the engine
  compiles without running extensions' compile hooks (`php -r`). A long C call delays it
  until it returns. The interval counts from the start of the previous call.
- `#[\phasync\Uninterruptible]` is not mutual exclusion: such a function can
  still suspend (on I/O, say), and other coroutines run, and are preempted,
  meanwhile.
- It is skipped where suspending would throw `FiberError` (pcntl signal
  handlers, a fiber being destroyed); the next interval gets it.
- An exception `$fn` throws surfaces at the interrupted point, in the
  interrupted code, as a `pcntl_async_signals()` handler's does.
- It is not re-entrant: never called while a call of it runs in the same fiber
  (or outside any fiber). A call suspended in one fiber doesn't hold back calls
  in others.
- It returns the previous closure; `null` removes it and stops the thread. With
  none set, there is no thread, and checkpoints only load the flag. Other interrupt users
  (`pcntl_async_signals()`, `max_execution_time`) keep working.
- It is per process (per thread on ZTS builds). A `fork()`ed child keeps the
  closure and gets a timer thread of its own.

A checkpoint the stack rules reject lowers the flag; the thread raises it again
0.25 ms later.

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
