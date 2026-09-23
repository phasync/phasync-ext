--TEST--
ensure_loaded() re-execs a CLI process without the extension into one with it
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (stripos(PHP_OS, 'WIN') === 0) die('skip POSIX only');
if (!function_exists('pcntl_exec') && !class_exists('FFI')) die('skip needs pcntl_exec or FFI');
if (!function_exists('shell_exec')) die('skip needs shell_exec');
if (!is_file(dirname(__DIR__) . '/modules/phasync.so')) die('skip needs local modules/phasync.so');
?>
--FILE--
<?php
$so   = dirname(__DIR__) . '/modules/phasync.so';
$boot = dirname(__DIR__) . '/phasync-ext.php';

// A script that, run WITHOUT the extension, should re-exec itself into one that
// has it and then report it as loaded.
$script = tempnam(sys_get_temp_dir(), 'phel');
file_put_contents($script,
    '<?php require ' . var_export($boot, true) . ';'
    . ' \\phasync\\ext\\ensure_loaded(' . var_export($so, true) . ');'
    . ' echo extension_loaded("phasync") ? "loaded\n" : "missing\n";'
);

// Run it under a plain PHP that does NOT have phasync loaded.
$out = shell_exec(escapeshellarg(PHP_BINARY) . ' ' . escapeshellarg($script) . ' 2>&1');
@unlink($script);
echo $out;
?>
--EXPECT--
loaded
