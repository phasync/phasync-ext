--TEST--
virtualize(): exit()/die() end the request, not the worker, in the fiber running virtualize() or one started inside; shutdown functions run
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/sink.inc';
use function phasync\ext\virtualize;

class ExitSink extends Sink
{
    public array $exits = [];
    public function exit(int|string $status): void { $this->exits[] = $status; }
}

// In the fiber running virtualize(): the rest of $code is skipped, the
// request's shutdown functions run, virtualize() returns null.
$s = new ExitSink;
$r = virtualize(function () {
    register_shutdown_function(function () { echo " shutdown"; });
    echo "before ";
    exit("bye");
    echo "never";
}, $s);
var_dump($r, $s->out, $s->exits);

// die() is the same function.
$s = new ExitSink;
virtualize(function () { die(7); }, $s);
var_dump($s->exits);

// In a fiber started inside: that fiber ends quietly (getReturn() null), the
// rest of the request goes on, and $sapi->exit() lets the server end it.
$s = new ExitSink;
virtualize(function () {
    $f = new Fiber(function () { echo "child "; exit(3); echo "never"; });
    $f->start();
    var_dump($f->isTerminated(), $f->getReturn());
    echo "parent continues";
}, $s);
var_dump($s->out, $s->exits);

// exit() in a shutdown function stops the remaining ones, as natively.
$s = new ExitSink;
virtualize(function () {
    register_shutdown_function(function () { echo "first"; exit; });
    register_shutdown_function(function () { echo "second"; });
}, $s);
var_dump($s->out);

// The worker's own shutdown functions and exit status are untouched.
register_shutdown_function(function () { echo "worker shutdown\n"; });
$code = <<<'PHP'
    require $argv[1];
    phasync\ext\virtualize(function () { exit(5); }, new Sink);
PHP;
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg(realpath(ini_get('extension_dir') . '/phasync.so'))
    . ' -r ' . escapeshellarg($code) . ' ' . escapeshellarg(__DIR__ . '/sink.inc'), $status);
echo "worker exit status: $status\n";
echo "worker goes on\n";
?>
--EXPECTF--
NULL
string(%d) "before bye shutdown"
array(1) {
  [0]=>
  string(3) "bye"
}
array(1) {
  [0]=>
  int(7)
}
string(%d) "child bool(true)
NULL
parent continues"
array(1) {
  [0]=>
  int(3)
}
string(5) "first"
worker exit status: 0
worker goes on
worker shutdown
