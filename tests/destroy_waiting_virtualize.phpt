--TEST--
virtualize(): a request's coroutine destroyed (by refcount or by the cycle collector) while it waits ends the request as exit() does (shutdown functions run, output and headers reach $sapi) and releases its state; the worker's state is intact and the next request works
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/destroy.inc';
require __DIR__ . '/sink.inc';
use function phasync\ext\virtualize;

// (The superglobals are the server's to swap, not part of this.)
function worker_state(): string {
    return json_encode([headers_list(), ob_get_level(), http_response_code(), connection_status()]);
}
$before = worker_state();

foreach (['refcount', 'gc'] as $how) {
    // The coroutine running virtualize(), destroyed while the request sleeps.
    echo "request coroutine ($how)\n";
    $s = new Sink;
    $s->info = ['query_string' => 'req=1'];
    destroy_while_waiting(new Loop, $how, fn() => virtualize(function () {
        register_shutdown_function(function () { echo " shutdown"; });
        header('X-Req: 1');
        ob_start();
        echo "partial";
        usleep(5000000);
        echo "never";
    }, $s));
    echo "  sapi: ", json_encode([$s->out, $s->headers[2] ?? null]), "\n";
    echo "  worker intact: ", json_encode(worker_state() === $before), "\n";

    // A coroutine started inside the request, destroyed with its locals (refcount)
    // or, after the request ended, by the collector: then its output is dropped.
    echo "child of an ended request ($how)\n";
    $s = new Sink;
    $loop = new Loop;
    $loop->manage(function () use ($loop, $s, $how) {
        virtualize(function () use ($loop, $how) {
            $holder = new stdClass;
            $child = new Fiber(function () use ($holder) {
                try { usleep(5000000); } finally { echo " child finally"; }
            });
            if ($how === 'gc') $holder->child = $child;
            $child->start();
            $loop->drop($child);
            echo "request done";
        }, $s);
        $n = gc_collect_cycles();
        if ($how === "gc") echo "  collected: ", $n > 0 ? "yes" : "no", "\n";
    });
    echo "  sapi: ", json_encode($s->out), "\n";
    echo "  worker intact: ", json_encode(worker_state() === $before), "\n";
}

// The request's output can't reach a $sapi that would have to wait (the peer
// isn't reading): it is lost, and the unwind goes on.
class SocketSapi
{
    public function __construct(public $sock) {}
    public function ub_write(string $data): bool { return fwrite($this->sock, $data) === strlen($data); }
    public function send_headers(int $status, ?string $line, array $headers): void {}
}
foreach (['refcount', 'gc'] as $how) {
    echo "output to a full socket ($how)\n";
    [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    stream_set_blocking($a, false);
    while (@fwrite($a, str_repeat('x', 65536))) {}
    stream_set_blocking($a, true);
    destroy_while_waiting(new Loop, $how, fn() => virtualize(function () {
        ob_start();                         // written as the request ends
        echo str_repeat('y', 100000);
        usleep(5000000);
    }, new SocketSapi($a)));
    echo "  worker intact: ", json_encode(worker_state() === $before), "\n";
    fclose($a);
    fclose($b);
}

// The next request works.
$s = new Sink;
virtualize(function () { header('X-Next: 1'); echo "next"; }, $s);
echo "next request: ", json_encode([$s->out, $s->headers[2]]), "\n";
echo "worker intact: ", json_encode(worker_state() === $before), "\n";
?>
--EXPECT--
request coroutine (refcount)
  finally ran
  sapi: ["partial shutdown",["X-Req: 1","Content-type: text\/html; charset=UTF-8"]]
  worker intact: true
child of an ended request (refcount)
  sapi: "request done child finally"
  worker intact: true
request coroutine (gc)
  finally ran
  collected: yes
  sapi: ["partial shutdown",["X-Req: 1","Content-type: text\/html; charset=UTF-8"]]
  worker intact: true
child of an ended request (gc)
  collected: yes
  sapi: "request done"
  worker intact: true
output to a full socket (refcount)
  finally ran
  worker intact: true
output to a full socket (gc)
  finally ran
  collected: yes
  worker intact: true
next request: ["next",["X-Next: 1","Content-type: text\/html; charset=UTF-8"]]
worker intact: true