<?php
use function phasync\ext\set_preempt_function;

$calls = 0;
set_preempt_function(function () use (&$calls) { $calls++; }, 0.005);

// A loop of 0.1 s; the number of calls of the closure during it.
function spin(): int
{
    global $calls;
    $calls = 0;
    $t = microtime(true);
    while (microtime(true) - $t < 0.1);
    return $calls;
}

$results = [];
function report(string $name, int $calls): void
{
    global $results;
    $results[] = "$name: " . ($calls >= 3 ? 'interrupted' : "calls: $calls");
}

// First, as it must run before any output.
class Handler implements SessionHandlerInterface
{
    public function open(string $path, string $name): bool { return true; }
    public function close(): bool { return true; }
    public function read(string $id): string|false { report('session read handler', spin()); return ''; }
    public function write(string $id, string $data): bool { return true; }
    public function destroy(string $id): bool { return true; }
    public function gc(int $max): int|false { return 0; }
}
session_set_save_handler(new Handler, false);
session_id('abc');
session_start();
session_write_close();

report('plain PHP code', spin());
$a = [2, 1];                                   // one comparison
usort($a, function ($x, $y) { report('usort comparator', spin()); return $x <=> $y; });
array_map(function ($x) { report('array_map callback', spin()); }, [1]);
ob_start(function (string $buf) { report('output handler', spin()); return $buf; });
echo '';
ob_end_flush();

set_error_handler(function () { report('error handler', spin()); return true; });
$x = $undefined;                               // raised by the engine
trigger_error('x', E_USER_NOTICE);             // raised by a C function
restore_error_handler();

class D { public function __destruct() { report('destructor', spin()); } }
$d = new D;
unset($d);

class Magic { public function __get($name) { report('__get', spin()); return 1; } }
(new Magic)->prop;

class It implements Iterator
{
    private int $i = 0;
    public function current(): mixed { report('Iterator::current() in foreach', spin()); return $this->i; }
    public function key(): mixed { return $this->i; }
    public function next(): void { $this->i++; }
    public function rewind(): void { $this->i = 0; }
    public function valid(): bool { return $this->i < 1; }
}
foreach (new It as $v);

class Wrapper
{
    public $context;
    public function stream_open($path, $mode, $options, &$opened) { return true; }
    public function stream_read($count) { report('stream wrapper read', spin()); return ''; }
    public function stream_eof() { return true; }
}
stream_wrapper_register('spin', Wrapper::class);
fread(fopen('spin://x', 'r'), 10);

function viaPlainCalls(): int { return spin(); }
report('a function called from PHP code', viaPlainCalls());

echo implode("\n", $results), "\n";

register_shutdown_function(function () { echo 'shutdown function: calls: ', spin(), "\n"; });
set_exception_handler(function () { echo 'exception handler: calls: ', spin(), "\n"; });
throw new Exception;
