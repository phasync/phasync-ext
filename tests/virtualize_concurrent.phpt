--TEST--
virtualize(): interleaved requests keep their own output, headers and handlers; fibers join the boundary of whoever starts them; $sapi methods run outside
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/sink.inc';
use function phasync\ext\virtualize;

// Two requests suspended half-way, in turn: each sees only its own state.
$a = new Sink; $b = new Sink;
$fa = new Fiber(fn() => virtualize(function () {
    header('X-Who: a');
    set_error_handler(function ($no, $msg) { echo "[a handles: $msg]"; return true; });
    echo "a1 ";
    Fiber::suspend();
    trigger_error('in a');
    echo "a2 ", count(headers_list());
}, $a));
$fb = new Fiber(fn() => virtualize(function () {
    header('X-Who: b');
    ob_start();
    echo "b1 ";
    Fiber::suspend();
    echo "b2 ", json_encode(headers_list());
}, $b));
$fa->start(); $fb->start();
echo "worker between: ", json_encode(headers_list()), "\n";
$fa->resume(); $fb->resume();
var_dump($a->out, $b->out, $a->headers[2][0], $b->headers[2][0]);
trigger_error('the worker\'s own error handler is untouched', E_USER_NOTICE);

// A fiber started inside belongs to the request, even if created outside;
// one started by the worker does not, even if created inside.
$s = new Sink;
$outside = new Fiber(function () { echo "created outside, started inside\n"; });
$inside = null;
virtualize(function () use ($outside, &$inside) {
    $outside->start();
    $inside = new Fiber(function () { echo "created inside, started outside\n"; });
}, $s);
$inside->start();
echo $s->out;

// ub_write() suspending: the other request runs meanwhile.
$a = new Sink; $b = new Sink;
$a->onWrite = function ($d) { Fiber::suspend("a suspended writing '$d'"); };
$fa = new Fiber(fn() => virtualize(function () { echo "A1"; echo "A2"; return 'ra'; }, $a));
$fb = new Fiber(fn() => virtualize(function () { echo "B1"; return 'rb'; }, $b));
var_dump($fa->start());
$fb->start();
while (!$fa->isTerminated()) $fa->resume();
var_dump($fa->getReturn(), $fb->getReturn(), $a->out, $b->out);

// $sapi methods run outside the boundary: their echo goes to the worker.
$loud = new class extends Sink {
    public function send_headers(int $s, ?string $l, array $h): void { echo "send_headers() speaking in the worker\n"; }
};
virtualize(function () { echo "x"; }, $loud);
var_dump($loud->out);

// A fiber still running when its request is over: output discarded.
$s = new Sink; $stray = null;
virtualize(function () use (&$stray) {
    $stray = new Fiber(function () { Fiber::suspend(); echo "late"; });
    $stray->start();
    echo "done";
}, $s);
$stray->resume();
var_dump($s->out);

// Uncaught exception: the request's exception handler gets it, as at the top of a script.
$s = new Sink;
var_dump(virtualize(function () {
    set_exception_handler(function ($e) { echo "handled: ", $e->getMessage(); });
    throw new RuntimeException('boom');
}, $s), $s->out);

// Without one, it propagates after the request ended (buffers flushed).
$s = new Sink;
try {
    virtualize(function () { ob_start(); echo "partial"; throw new RuntimeException('up'); }, $s);
} catch (RuntimeException $e) {
    echo "caught: ", $e->getMessage(), "\n";
}
var_dump($s->out);

// Nesting throws; a $sapi without the required methods is refused.
try {
    virtualize(fn() => virtualize(fn() => 1, new Sink), new Sink);
} catch (Error $e) {
    echo get_class($e), ": ", $e->getMessage(), "\n";
}
try {
    virtualize(fn() => 1, new stdClass);
} catch (ValueError $e) {
    echo get_class($e), ": ", $e->getMessage(), "\n";
}
?>
--EXPECTF--
worker between: []
string(%d) "a1 [a handles: in a]a2 2"
string(%d) "b1 b2 ["X-Who: b"]"
string(8) "X-Who: a"
string(8) "X-Who: b"

Notice: the worker's own error handler is untouched in %s on line %d
created inside, started outside
created outside, started inside
string(24) "a suspended writing 'A1'"
string(2) "ra"
string(2) "rb"
string(4) "A1A2"
string(2) "B1"
send_headers() speaking in the worker
string(1) "x"
string(4) "done"
NULL
string(13) "handled: boom"
caught: up
string(7) "partial"
Error: phasync\ext\virtualize() cannot be nested
ValueError: phasync\ext\virtualize(): Argument #2 ($sapi) must have the methods ub_write() and send_headers()
