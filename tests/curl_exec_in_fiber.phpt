--TEST--
curl_exec() inside a scope runs on a private curl_multi and waits cooperatively; results, errors and outputs match native
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
// The body runs in a child PHP with the normal ini; skip if that has no ext/curl.
if (trim((string) shell_exec(escapeshellarg(PHP_BINARY) . ' -r "echo function_exists(\'curl_exec\') ? 1 : 0;"')) !== '1') die('skip requires ext/curl');
?>
--FILE--
<?php
// A plain HTTP server in its own process: /hello after 300ms, /missing -> 404.
$server = '
$s = stream_socket_server("tcp://127.0.0.1:0");
$n = stream_socket_get_name($s, false); echo substr($n, strrpos($n, ":") + 1), "\n"; flush();
while ($c = @stream_socket_accept($s, 20)) {
    $req = fread($c, 8192);
    usleep(300000);
    if (str_contains($req, "GET /missing")) fwrite($c, "HTTP/1.0 404 Not Found\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope");
    else fwrite($c, "HTTP/1.0 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello");
    fclose($c);
}';
$srv = proc_open([PHP_BINARY, '-n', '-r', $server], [1 => ['pipe', 'w']], $pipes);
$port = trim(fgets($pipes[1]));

$child = tempnam(sys_get_temp_dir(), 'phasync_cx_') . '.php';
file_put_contents($child, '<?php $port = ' . (int) $port . '; require ' . var_export(__DIR__ . '/loop.inc', true) . ';' . <<<'CHILD'

// Run $f natively, then in a coroutine beside a ticker: compare, and check that
// the ticker kept running during the (300ms) request.
function check(string $name, Closure $f, bool $slow = true): void {
    ob_start(); $native = $f(); $native = [$native, ob_get_clean()];
    $ticks = 0; $done = false; $ext = null;
    (new Loop)->runAll(
        function () use ($f, &$ext, &$done) { ob_start(); $r = $f(); $ext = [$r, ob_get_clean()]; $done = true; },
        function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } },
    );
    echo rtrim(sprintf("%-22s %s %s", $name, $native === $ext ? 'same' : 'DIFF ' . json_encode([$native, $ext]),
        !$slow ? '' : ($ticks >= 5 ? 'cooperative' : "BLOCKED (ticks=$ticks)"))), "\n";
}
function req(string $path, array $opts = []): CurlHandle {
    global $port;
    $ch = curl_init("http://127.0.0.1:$port$path");
    curl_setopt_array($ch, $opts);
    return $ch;
}

check('RETURNTRANSFER', function () { $ch = req('/hello', [CURLOPT_RETURNTRANSFER => true]); return [curl_exec($ch), curl_errno($ch), curl_getinfo($ch, CURLINFO_RESPONSE_CODE)]; });
check('echo output', function () { $ch = req('/hello'); return curl_exec($ch); });
check('404', function () { $ch = req('/missing', [CURLOPT_RETURNTRANSFER => true]); return [curl_exec($ch), curl_getinfo($ch, CURLINFO_RESPONSE_CODE)]; });
check('CURLOPT_FILE', function () {
    $file = tempnam(sys_get_temp_dir(), 'cx'); $fp = fopen($file, 'w');
    $r = curl_exec(req('/hello', [CURLOPT_FILE => $fp]));
    $got = file_get_contents($file);                   // complete right after, as natively
    fclose($fp); unlink($file);
    return [$r, $got];
});
check('refused', function () { $ch = curl_init('http://127.0.0.1:1/'); curl_setopt($ch, CURLOPT_RETURNTRANSFER, true); return [curl_exec($ch), curl_errno($ch), curl_error($ch)]; }, false);
check('timeout', function () { $ch = req('/hello', [CURLOPT_RETURNTRANSFER => true, CURLOPT_TIMEOUT_MS => 100]); return [curl_exec($ch), curl_errno($ch)]; }, false);
check('handle in a multi', function () {
    $ch = req('/hello', [CURLOPT_RETURNTRANSFER => true]);
    $mh = curl_multi_init(); curl_multi_add_handle($mh, $ch);
    $r = [curl_exec($ch), curl_errno($ch)];
    curl_multi_remove_handle($mh, $ch);
    return $r;
}, false);

// Callbacks run on PHP's thread, inside the coroutine.
$inFiber = null;
(new Loop)->runAll(function () use (&$inFiber) {
    curl_exec(req('/hello', [CURLOPT_WRITEFUNCTION => function ($ch, $data) use (&$inFiber) { $inFiber = Fiber::getCurrent() !== null; return strlen($data); }]));
});
var_dump($inFiber);

// A cancelled request propagates the cancellation; the handle works afterwards.
$ch = req('/hello', [CURLOPT_RETURNTRANSFER => true]);
$l = new Loop;
$l->runAll(
    function () use ($ch) {
        $GLOBALS['w'] = Fiber::getCurrent();
        try { curl_exec($ch); echo "done?!\n"; } catch (LoopCancelled $e) { echo "request cancelled\n"; }
    },
    function () use ($l) { usleep(50000); $l->cancel($GLOBALS['w']); },
);
var_dump(curl_exec($ch));
CHILD);
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' ' . escapeshellarg($child) . ' 2>&1');
unlink($child);
proc_terminate($srv);
fclose($pipes[1]);
proc_close($srv);
?>
--EXPECT--
RETURNTRANSFER         same cooperative
echo output            same cooperative
404                    same cooperative
CURLOPT_FILE           same cooperative
refused                same
timeout                same
handle in a multi      same
bool(true)
request cancelled
string(5) "hello"
