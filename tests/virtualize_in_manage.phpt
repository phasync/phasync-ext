--TEST--
virtualize() inside manage(): concurrent requests whose $sapi writes to real sockets (parking when full) stay separate; exit() in a child lets the server cancel the request
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
use function phasync\ext\virtualize;

// A $sapi writing the response to a socket, as a server does: fwrite() parks the
// coroutine through the Poller whenever the peer isn't reading.
class SocketSapi
{
    public function __construct(public $sock, public ?Closure $onExit = null) {}
    public function ub_write(string $data): bool { return fwrite($this->sock, $data) === strlen($data); }
    public function send_headers(int $status, ?string $line, array $headers): void {
        fwrite($this->sock, "$status " . implode('|', $headers) . "\n");
    }
    public function exit(int|string $status): void { if ($this->onExit) ($this->onExit)(); }
}

$loop = new Loop;
$got = [];
$reqs = [];
foreach (['a', 'b', 'c'] as $name) {
    [$server, $client] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
    // The request: 200 KB in pieces, so the socket fills and ub_write() parks.
    $reqs[] = $loop->go(function () use ($server, $name) {
        virtualize(function () use ($name) {
            header("X-Req: $name");
            for ($i = 0; $i < 50; $i++) {
                echo str_repeat($name, 4000);
            }
        }, new SocketSapi($server));
        fclose($server);
    });
    // The client reads slowly.
    $loop->go(function () use ($client, $name, &$got) {
        $data = '';
        while (!feof($client)) {
            $data .= fread($client, 65536);
            usleep(2000);
        }
        $got[$name] = $data;
    });
}
$loop->manage(fn() => $loop->run());
ksort($got);
foreach ($got as $name => $data) {
    [$head, $body] = explode("\n", $data, 2);
    echo "$name: ", substr($head, 0, strpos($head, '|')), " ", strlen($body), " bytes, only '$name': ",
        var_export($body === str_repeat($name, 200000), true), "\n";
}
echo "worker parks: ", $loop->parks > 0 ? "yes" : "no", "\n";

// exit() in a child coroutine: the child ends, $sapi->exit() cancels the rest.
// (phasync starts a coroutine at once, from the fiber creating it, so the
// child joins the request; this Loop only queues starts, so start it here.)
$loop = new Loop;
[$server, $client] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
$owner = null;
$owner = $loop->go(function () use ($loop, $server, &$owner) {
    try {
        virtualize(function () {
            register_shutdown_function(function () { echo "shutdown ran"; });
            (new Fiber(function () { echo "child exits. "; exit; }))->start();
            while (true) {
                Fiber::suspend();         // waiting for more work, until cancelled
            }
        }, new SocketSapi($server, fn() => $loop->cancel($owner)));
    } catch (LoopCancelled $e) {
        echo "request cancelled\n";
    }
    fclose($server);
});
$loop->manage(fn() => $loop->run());
echo substr(stream_get_contents($client), 4), "\n";
?>
--EXPECTF--
a: 200 X-Req: a 200000 bytes, only 'a': true
b: 200 X-Req: b 200000 bytes, only 'b': true
c: 200 X-Req: c 200000 bytes, only 'c': true
worker parks: yes
request cancelled
Content-type: text/html; charset=UTF-8
child exits. shutdown ran
