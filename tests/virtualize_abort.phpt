--TEST--
virtualize(): a client gone (ub_write() false, or connection_aborted() saying so) aborts the request as natively, honouring ignore_user_abort()
--EXTENSIONS--
phasync
--FILE--
<?php
require __DIR__ . '/sink.inc';
use function phasync\ext\virtualize;

// ub_write() false: aborted, later output discarded, the request ends (shutdown
// functions still run and see the abort).
$s = new Sink; $s->gone = true;
$seen = null;
virtualize(function () use (&$seen) {
    register_shutdown_function(function () use (&$seen) { $seen = [connection_aborted(), connection_status()]; });
    echo "one";
    echo "two";
    $seen = 'never';
}, $s);
var_dump($s->out, $seen);

// With ignore_user_abort(true) the code runs on, without output.
$s = new Sink; $s->gone = true;
$log = [];
virtualize(function () use (&$log) {
    ignore_user_abort(true);
    echo "one";
    $log[] = connection_aborted();
    echo "two";
    $log[] = 'still running';
}, $s);
var_dump($s->out, $log);
var_dump(ignore_user_abort(), connection_status());   // the worker's own

// A server that knows the client left before any write fails says so.
$s = new class extends Sink { public function connection_aborted(): bool { return true; } };
$log = [];
virtualize(function () use (&$log) { $log = [connection_aborted(), connection_status()]; }, $s);
var_dump($log);
?>
--EXPECT--
string(3) "one"
array(2) {
  [0]=>
  int(1)
  [1]=>
  int(1)
}
string(3) "one"
array(2) {
  [0]=>
  int(1)
  [1]=>
  string(13) "still running"
}
int(0)
int(0)
array(2) {
  [0]=>
  int(1)
  [1]=>
  int(1)
}
