<?php

/*
 * Composer files-autoloaded bootstrap for the phasync extension.
 *
 * A C extension cannot load itself from inside its own not-yet-loaded code, so
 * activation is done here, in plain PHP, by re-executing the current CLI process
 * with the extension loaded the normal way (`-d extension=…`, i.e. at MINIT).
 * That is the only fully ABI-safe load path — no FFI self-registration, no dl()
 * temporary-module lifecycle.
 *
 * Call phasync\ext\ensure_loaded() once, as early as possible, before doing any
 * work with side effects (it may replace the process). It is a no-op when the
 * extension is already present (installed via PIE, `extension=` in php.ini, or a
 * previous re-exec).
 */

declare(strict_types=1);

namespace phasync\ext;

if (!\function_exists('phasync\\ext\\ensure_loaded')) {

    /**
     * Ensure the phasync extension is loaded, re-executing this CLI process with
     * `-d extension=<matching .so>` if it is not.
     *
     * @param string|null $soPath  Explicit path to the .so to load. When null,
     *     the path is taken from the PHASYNC_EXT_SO environment variable, else a
     *     per-ABI file under bin/, else a local dev build under modules/.
     * @throws \RuntimeException if the extension is absent and cannot be loaded
     *     (no re-exec primitive, wrong SAPI, no matching binary, or the re-exec
     *     ran but the extension still did not load).
     */
    function ensure_loaded(?string $soPath = null): void
    {
        if (\extension_loaded('phasync')) {
            return;   // already active (PIE / extension= / a prior re-exec)
        }

        // Loop guard: if we already re-execed and the extension still is not
        // here, the .so is missing or ABI-mismatched — do not re-exec again.
        if (\getenv('PHASYNC_EXT_REEXEC') === '1') {
            throw new \RuntimeException(
                'phasync\\ext\\ensure_loaded(): re-exec did not load the phasync '
                . 'extension. The selected binary is missing or does not match this '
                . 'PHP build (' . _abi_key() . ').'
            );
        }

        if (\PHP_SAPI !== 'cli') {
            throw new \RuntimeException(
                'phasync\\ext\\ensure_loaded(): the extension is not loaded and can '
                . 'only be auto-loaded on the CLI SAPI. Add "extension=phasync" to '
                . 'php.ini for this SAPI (' . \PHP_SAPI . ').'
            );
        }

        $so = $soPath ?? _resolve_so();
        if ($so === null || !\is_file($so)) {
            throw new \RuntimeException(
                "phasync\\ext\\ensure_loaded(): no prebuilt phasync binary is bundled "
                . "for this platform (" . _abi_key() . ").\n"
                . "Bundled platforms: PHP 8.3/8.4/8.5, x86_64/aarch64, glibc/musl.\n"
                . "Build it yourself and point PHASYNC_EXT_SO at the result:\n"
                . "    git clone https://github.com/phasync/phasync-ext\n"
                . "    cd phasync-ext && phpize && ./configure --enable-phasync && make\n"
                . "    PHASYNC_EXT_SO=\"\$PWD/modules/phasync.so\" php your-app.php\n"
                . "(or drop it in " . __DIR__ . "/bin/phasync-" . _abi_key() . ".so, "
                . "or add 'extension=<path>' to php.ini)."
            );
        }

        // Rebuild the exact command line (preserving the user's own -d flags etc.)
        // and insert our extension flag right after the interpreter.
        $args = _original_args();                 // interpreter args + script + argv
        $extra = ['-d', 'extension=' . $so];
        \putenv('PHASYNC_EXT_REEXEC=1');          // inherited by the re-exec'd child

        if (\function_exists('pcntl_exec')) {
            // pcntl_exec sets argv[0] = PHP_BINARY itself.
            pcntl_exec(\PHP_BINARY, \array_merge($extra, $args));
            // only reached if exec failed
        }

        if (\class_exists(\FFI::class) && _ffi_execv(\array_merge([\PHP_BINARY], $extra, $args))) {
            // _ffi_execv does not return on success
        }

        throw new \RuntimeException(
            'phasync\\ext\\ensure_loaded(): cannot re-exec to load the extension '
            . '(neither pcntl_exec() nor FFI execv() is available). Run with '
            . '"php -d extension=' . $so . ' …" instead.'
        );
    }

    /** ABI key for this PHP build, e.g. "8.5-nts-x86_64-glibc". @internal */
    function _abi_key(): string
    {
        return \PHP_MAJOR_VERSION . '.' . \PHP_MINOR_VERSION
            . '-' . (\PHP_ZTS ? 'zts' : 'nts')
            . '-' . \php_uname('m')
            . '-' . (\glob('/lib/ld-musl-*.so.1') ? 'musl' : 'glibc');
    }

    /** Resolve the .so to load: env override, per-ABI bin/, then dev modules/. @internal */
    function _resolve_so(): ?string
    {
        $env = \getenv('PHASYNC_EXT_SO');
        if (\is_string($env) && $env !== '') {
            return $env;
        }
        $candidates = [
            __DIR__ . '/bin/phasync-' . _abi_key() . '.so',
            __DIR__ . '/modules/phasync.so',   // local dev build (assumes matching ABI)
        ];
        foreach ($candidates as $c) {
            if (\is_file($c)) {
                return $c;
            }
        }
        return null;
    }

    /**
     * The real interpreter argv (including flags the user passed before the
     * script), minus argv[0]. Uses /proc/self/cmdline where available so that
     * flags like -d memory_limit=… survive the re-exec; falls back to the script
     * argv (which drops interpreter flags). @internal
     *
     * @return list<string>
     */
    function _original_args(): array
    {
        $raw = @\file_get_contents('/proc/self/cmdline');
        if ($raw !== false && $raw !== '') {
            $parts = \explode("\0", \rtrim($raw, "\0"));
            \array_shift($parts);   // drop argv[0] (the interpreter)
            return $parts;
        }
        // Fallback: script + its args (interpreter flags are not recoverable here).
        return $_SERVER['argv'] ?? [];
    }

    /**
     * Replace the current process via libc execv() through FFI. Returns false if
     * FFI is unusable; does not return on success. @internal
     *
     * @param list<string> $argv  full argv including argv[0]
     */
    function _ffi_execv(array $argv): bool
    {
        try {
            $ffi = \FFI::cdef('int execv(const char *path, char *const argv[]);');
        } catch (\Throwable) {
            return false;   // ffi.enable=0, or cdef blocked
        }
        $n = \count($argv);
        // owned=false: we are about to replace the image, so nothing needs freeing.
        $c = \FFI::new("char*[" . ($n + 1) . "]", false);
        $keep = [];
        foreach ($argv as $i => $s) {
            $buf = \FFI::new('char[' . (\strlen($s) + 1) . ']', false);
            \FFI::memcpy($buf, $s, \strlen($s));
            $buf[\strlen($s)] = "\0";
            $c[$i] = \FFI::cast('char *', $buf);
            $keep[] = $buf;
        }
        $c[$n] = null;
        $ffi->execv(\PHP_BINARY, $c);
        return false;   // execv returned -> it failed
    }
}
