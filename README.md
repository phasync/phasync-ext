# phasync-ext

A PHP extension that gives [phasync](https://github.com/phasync/phasync) — and any
fiber-based async code — two things on **PHP 8.3+**, without patching PHP:

1. **`phasync\stream_select()`** — a drop-in `stream_select()` that uses `poll(2)`
   internally, so it is **not bounded by `FD_SETSIZE`** (the ~1024 descriptor
   ceiling). Accepts stream resources *and* plain integer file descriptors.

2. **Transparent async I/O hooks** — `phasync\enable_hooks()` makes ordinary
   blocking calls cooperatively yield the current fiber instead of blocking the
   process. It covers:
   - `tcp://` / `unix://` sockets (incl. `fsockopen`, `stream_socket_client/server`)
   - `ssl://` / `tls://` sockets
   - `proc_open()` pipes
   - `sleep()`, `usleep()`, `time_nanosleep()`, `time_sleep_until()`

   The extension performs the real I/O; on a would-block it invokes a userland
   callback that decides how to wait (typically `Fiber::suspend()` into a
   scheduler). The C side never touches the Fiber API — your callback owns all
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

## API

```php
namespace phasync;

function stream_select(?array &$read, ?array &$write, ?array &$except,
                       ?int $seconds, ?int $microseconds = null): int|false;

function register_read_handler(?callable $handler): void;   // ($fd) — wait until readable
function register_write_handler(?callable $handler): void;  // ($fd) — wait until writable
function register_sleep_handler(?callable $handler): void;  // ($microseconds) — wait that long

function enable_hooks(): void;    // install transport + function hooks
function disable_hooks(): void;   // restore originals
```

The handlers receive an integer fd (or a µs duration for sleep) and are
responsible only for *waiting* — the extension does the actual I/O afterwards. A
handler that is called outside an event loop can just do a small blocking
`phasync\stream_select()` to satisfy the contract.

## Status

v1. Sockets, TLS, pipes and the sleep functions are implemented and tested.
Known TLS limitations: read/write intent is approximated (TLS renegotiation
wanting the opposite direction could stall — the `stream_socket_get_crypto_status()`
API on 8.5+ would resolve it), the `ssl://` handshake still runs synchronously,
and `stream_socket_enable_crypto()` on an already-open socket is not yet re-wrapped.

Linux only for now (uses `poll(2)` and `fcntl`).

## License

MIT — see [LICENSE](LICENSE).
