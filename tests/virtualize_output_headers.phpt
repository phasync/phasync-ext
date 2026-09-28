--TEST--
virtualize(): output and response headers are the request's own and reach $sapi as a SAPI receives them
--EXTENSIONS--
phasync
--FILE--
<?php
require __DIR__ . '/sink.inc';
use function phasync\ext\virtualize;

// Output passes the buffers; headers go once, before it, with PHP's default Content-type.
$s = new Sink;
$r = virtualize(function () {
    header('X-A: 1');
    http_response_code(201);
    setcookie('c', 'v');
    echo "hello ";
    ob_start(null, 0, 0);            // not removable: flushed anyway when the request ends
    echo "buffered";
    return 42;
}, $s);
var_dump($r, $s->out, $s->headers);

// None of it touched the worker's own state.
var_dump(headers_list(), http_response_code());

// The status line, header_register_callback() just before sending, flush().
$s = new Sink;
virtualize(function () {
    header_register_callback(function () { header('X-Late: yes'); });
    header('HTTP/1.1 404 Gone Fishing');
    echo "x";
    flush();
}, $s);
var_dump($s->headers, $s->flushes);

// After output, header() warns as natively, naming where output started; the
// warning is displayed inside the request, so it is part of its output.
$s = new Sink;
virtualize(function () { echo "x"; header('X-Too: late'); var_dump(headers_sent($file, $line), $line); }, $s);
echo $s->out;

// A request with no output still sends its headers.
$s = new Sink;
virtualize(function () { header('Location: /elsewhere'); }, $s);
var_dump($s->headers[0], $s->out);

// HEAD: headers only, as sapi_activate() does.
$s = new Sink;
$s->info = ['method' => 'HEAD'];
virtualize(function () { echo "body"; }, $s);
var_dump($s->out, $s->headers !== null);
?>
--EXPECTF--
int(42)
string(14) "hello buffered"
array(3) {
  [0]=>
  int(201)
  [1]=>
  NULL
  [2]=>
  array(3) {
    [0]=>
    string(6) "X-A: 1"
    [1]=>
    string(15) "Set-Cookie: c=v"
    [2]=>
    string(%d) "Content-type: text/html; charset=UTF-8"
  }
}
array(0) {
}
bool(false)
array(3) {
  [0]=>
  int(404)
  [1]=>
  string(25) "HTTP/1.1 404 Gone Fishing"
  [2]=>
  array(2) {
    [0]=>
    string(%d) "Content-type: text/html; charset=UTF-8"
    [1]=>
    string(11) "X-Late: yes"
  }
}
int(1)
x
Warning: Cannot modify header information - headers already sent by (output started at %s:%d) in %s on line %d
bool(true)
int(%d)
int(302)
string(0) ""
string(0) ""
bool(true)
