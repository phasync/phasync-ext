--TEST--
virtualize(): php://input, request_parse_body() and uploads read the request's own body through read_post(); uploads not moved are deleted when it ends
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (PHP_VERSION_ID < 80400) die('skip request_parse_body() needs PHP 8.4'); ?>
--FILE--
<?php
require __DIR__ . '/sink.inc';
use function phasync\ext\virtualize;

// php://input, readable more than once.
$s = new Sink;
$s->body = 'a=1&b[]=2&b[]=3';
$s->info = ['method' => 'POST', 'content_type' => 'application/x-www-form-urlencoded', 'content_length' => 15];
$got = [];
virtualize(function () use (&$got) { $got = [file_get_contents('php://input'), file_get_contents('php://input')]; }, $s);
var_dump($got);

// request_parse_body() with PHP's own parser.
$s = new Sink;
$s->body = 'a=1&b[]=2&b[]=3';
$s->info = ['method' => 'PUT', 'content_type' => 'application/x-www-form-urlencoded', 'content_length' => 15];
virtualize(function () use (&$got) { [$got] = request_parse_body(); }, $s);
var_dump($got);

// Multipart: move_uploaded_file() works; a file not moved is gone afterwards.
$bd = 'XyZ';
$s = new Sink;
$s->body = "--$bd\r\nContent-Disposition: form-data; name=\"f\"; filename=\"a.txt\"\r\nContent-Type: text/plain\r\n\r\nfirst file\r\n"
    . "--$bd\r\nContent-Disposition: form-data; name=\"g\"; filename=\"b.txt\"\r\nContent-Type: text/plain\r\n\r\nsecond file\r\n--$bd--\r\n";
$s->info = ['method' => 'POST', 'content_type' => "multipart/form-data; boundary=$bd", 'content_length' => strlen($s->body)];
$dest = sys_get_temp_dir() . '/phasync_upload_' . getmypid();
$kept = null;
virtualize(function () use ($dest, &$got, &$kept) {
    [, $files] = request_parse_body();
    $kept = $files['g']['tmp_name'];
    $got = [is_uploaded_file($files['f']['tmp_name']), move_uploaded_file($files['f']['tmp_name'], $dest), file_exists($kept)];
}, $s);
var_dump($got, file_get_contents($dest), file_exists($kept), is_uploaded_file($kept));
unlink($dest);

// A php://input handle kept past the request is closed, not dangling.
$s = new Sink;
$s->body = 'xyz';
$s->info = ['method' => 'POST', 'content_type' => 'text/plain', 'content_length' => 3];
$h = null;
virtualize(function () use (&$h) { $h = fopen('php://input', 'r'); fread($h, 1); }, $s);
try { fread($h, 10); } catch (TypeError $e) { echo $e->getMessage(), "\n"; }

// php://input read first, then request_parse_body(): nothing left to parse,
// as natively (it starts a new body stream; the old one is freed at the end).
$s = new Sink;
$s->body = 'a=1';
$s->info = ['method' => 'POST', 'content_type' => 'application/x-www-form-urlencoded', 'content_length' => 3];
virtualize(function () use (&$got) { $got = [file_get_contents('php://input'), request_parse_body()[0]]; }, $s);
var_dump($got);

// Without read_post() the request has no body.
$plain = new class { public function ub_write(string $d): bool { return true; } public function send_headers(int $s, ?string $l, array $h): void {} };
virtualize(function () use (&$got) { $got = file_get_contents('php://input'); }, $plain);
var_dump($got);
?>
--EXPECTF--
array(2) {
  [0]=>
  string(15) "a=1&b[]=2&b[]=3"
  [1]=>
  string(15) "a=1&b[]=2&b[]=3"
}
array(2) {
  ["a"]=>
  string(1) "1"
  ["b"]=>
  array(2) {
    [0]=>
    string(1) "2"
    [1]=>
    string(1) "3"
  }
}
array(3) {
  [0]=>
  bool(true)
  [1]=>
  bool(true)
  [2]=>
  bool(true)
}
string(10) "first file"
bool(false)
bool(false)
fread(): %s
array(2) {
  [0]=>
  string(3) "a=1"
  [1]=>
  array(0) {
  }
}
string(0) ""
