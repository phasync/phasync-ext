--TEST--
virtualize(): $_GET, $_POST, $_COOKIE, $_SERVER, $_FILES and $_REQUEST are built from $sapi as PHP builds them per request; a server swapping the global variables around resuming a request's fibers keeps them the request's across suspensions and in fibers started inside
--EXTENSIONS--
phasync
filter
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--INI--
variables_order=EGPCS
request_order=GP
enable_post_data_reading=1
--FILE--
<?php
require __DIR__ . '/sink.inc';
use function phasync\ext\virtualize;

function seen(): string {
    // Read from a function, as most code does.
    return json_encode([
        'get' => $_GET, 'post' => $_POST, 'cookie' => $_COOKIE, 'request' => $_REQUEST,
        'server' => array_diff_key($_SERVER, ['REQUEST_TIME' => 1, 'REQUEST_TIME_FLOAT' => 1]),
        'files' => array_map(fn($f) => [$f['name'], $f['size'], is_uploaded_file($f['tmp_name'])], $_FILES),
        'filter_input' => filter_input(INPUT_GET, 'q'),
    ]);
}

// A: an urlencoded form POST.
$a = new Sink;
$a->body = 'name=alice&tags[]=x&tags[]=y';
$a->info = ['method' => 'POST', 'content_type' => 'application/x-www-form-urlencoded',
    'content_length' => strlen($a->body), 'query_string' => 'q=a&page=1', 'request_uri' => '/a?q=a&page=1'];
$a->cookie = 'sid=A; theme=dark';
$a->server = ['REQUEST_METHOD' => 'POST', 'QUERY_STRING' => 'q=a&page=1', 'HTTP_HOST' => 'a.example', 'REMOTE_ADDR' => '10.0.0.1'];

// B: a multipart POST with a file.
$bd = 'Bd';
$b = new Sink;
$b->body = "--$bd\r\nContent-Disposition: form-data; name=\"title\"\r\n\r\nhello\r\n"
    . "--$bd\r\nContent-Disposition: form-data; name=\"doc\"; filename=\"b.txt\"\r\nContent-Type: text/plain\r\n\r\nfile of b\r\n--$bd--\r\n";
$b->info = ['method' => 'POST', 'content_type' => "multipart/form-data; boundary=$bd",
    'content_length' => strlen($b->body), 'query_string' => 'q=b'];
$b->cookie = 'sid=B';
$b->server = ['REQUEST_METHOD' => 'POST', 'HTTP_HOST' => 'b.example', 'REMOTE_ADDR' => '10.0.0.2'];

$worker = [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST];
$moved = sys_get_temp_dir() . '/phasync_sg_' . getmypid();
$log = [];
$request = function (string $who) use (&$log, $moved) {
    return function () use ($who, &$log, $moved) {
        $log[] = "$who before: " . seen();
        Fiber::suspend();
        $log[] = "$who after:  " . seen();
        // A fiber started inside belongs to the request.
        $f = new Fiber(function () use ($who, &$log) {
            Fiber::suspend();
            $log[] = "$who fiber:  " . json_encode([$_GET['q'], $_COOKIE['sid'], $_SERVER['HTTP_HOST'], $GLOBALS['_POST'] !== []]);
        });
        $f->start();
        Fiber::suspend();
        $f->resume();
        if ($who === 'b') {
            $log[] = 'b moves its upload: ' . json_encode(move_uploaded_file($_FILES['doc']['tmp_name'], $moved));
        }
    };
};
$fa = new Fiber(fn() => virtualize($request('a'), $a));
$fb = new Fiber(fn() => virtualize($request('b'), $b));
// The worker swaps each request's global variables in around resuming it (sink.inc).
$ga = new RequestGlobals; $gb = new RequestGlobals;
$ga->run($fa); $gb->run($fb);
$log[] = 'worker between: ' . json_encode([$worker === [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST], filter_input(INPUT_GET, 'q')]);
$gb->run($fb); $ga->run($fa);
$ga->run($fa); $gb->run($fb);
echo implode("\n", $log), "\n";
echo file_get_contents($moved), "\n";
unlink($moved);
var_dump($worker === [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST]);

// $_SERVER: PHP's own REQUEST_TIME, no argv of the worker's command line.
$s = new Sink;
$g = new RequestGlobals;
$g->enter();
virtualize(function () use (&$got) {
    $got = [isset($_SERVER['argv']), is_int($_SERVER['REQUEST_TIME']), is_float($_SERVER['REQUEST_TIME_FLOAT'])];
}, $s);
$g->leave();
var_dump($got, isset($_SERVER['argv']));

// Without the optional methods: empty, but for PHP's REQUEST_TIME entries.
$plain = new class {
    public function ub_write(string $d): bool { return true; }
    public function send_headers(int $s, ?string $l, array $h): void {}
};
virtualize(function () use (&$got) {
    $got = [$_GET, $_POST, $_COOKIE, $_FILES, $_REQUEST, array_keys($_SERVER), filter_input(INPUT_COOKIE, 'sid')];
}, $plain);
echo json_encode($got), "\n";
// Not swapped, the request's superglobals are left in the global variables.
var_dump($worker === [$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST], $_GET === [] && !isset($_SERVER['argv']));
[$_GET, $_POST, $_COOKIE, $_SERVER, $_FILES, $_REQUEST] = $worker;

// A request assigning a superglobal changes its own only.
$g = new RequestGlobals;
$g->enter();
virtualize(function () { $_GET['added'] = 1; $_SERVER = []; $_COOKIE = null; }, new Sink);
$g->leave();
var_dump(isset($_GET['added']), $_SERVER === $worker[3], isset($_COOKIE));
?>
--EXPECTF--
a before: {"get":{"q":"a","page":"1"},"post":{"name":"alice","tags":["x","y"]},"cookie":{"sid":"A","theme":"dark"},"request":{"q":"a","page":"1","name":"alice","tags":["x","y"]},"server":{"REQUEST_METHOD":"POST","QUERY_STRING":"q=a&page=1","HTTP_HOST":"a.example","REMOTE_ADDR":"10.0.0.1"},"files":[],"filter_input":"a"}
b before: {"get":{"q":"b"},"post":{"title":"hello"},"cookie":{"sid":"B"},"request":{"q":"b","title":"hello"},"server":{"REQUEST_METHOD":"POST","HTTP_HOST":"b.example","REMOTE_ADDR":"10.0.0.2"},"files":{"doc":["b.txt",9,true]},"filter_input":"b"}
worker between: [true,null]
b after:  {"get":{"q":"b"},"post":{"title":"hello"},"cookie":{"sid":"B"},"request":{"q":"b","title":"hello"},"server":{"REQUEST_METHOD":"POST","HTTP_HOST":"b.example","REMOTE_ADDR":"10.0.0.2"},"files":{"doc":["b.txt",9,true]},"filter_input":"b"}
a after:  {"get":{"q":"a","page":"1"},"post":{"name":"alice","tags":["x","y"]},"cookie":{"sid":"A","theme":"dark"},"request":{"q":"a","page":"1","name":"alice","tags":["x","y"]},"server":{"REQUEST_METHOD":"POST","QUERY_STRING":"q=a&page=1","HTTP_HOST":"a.example","REMOTE_ADDR":"10.0.0.1"},"files":[],"filter_input":"a"}
a fiber:  ["a","A","a.example",true]
b fiber:  ["b","B","b.example",true]
b moves its upload: true
file of b
bool(true)
array(3) {
  [0]=>
  bool(false)
  [1]=>
  bool(true)
  [2]=>
  bool(true)
}
bool(true)
[[],[],[],[],[],["REQUEST_TIME_FLOAT","REQUEST_TIME"],null]
bool(false)
bool(true)
bool(false)
bool(true)
bool(true)
