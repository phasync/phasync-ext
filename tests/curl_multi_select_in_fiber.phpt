--TEST--
curl_multi_select() waits cooperatively inside a scope (curl-multi loops such as Guzzle's), returning like native
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
// The body runs in a child PHP with the normal ini; skip if that has no ext/curl.
if (trim((string) shell_exec(escapeshellarg(PHP_BINARY) . ' -r "echo function_exists(\'curl_multi_init\') ? 1 : 0;"')) !== '1') die('skip requires ext/curl');
?>
--FILE--
<?php
// ext/curl is usually a shared extension that run-tests' `-n` can't load, so the
// body runs in a child PHP with the normal ini and the extension loaded.
$child = tempnam(sys_get_temp_dir(), 'phasync_cm_') . '.php';
file_put_contents($child, '<?php require ' . var_export(__DIR__ . '/loop.inc', true) . ';' . <<<'CHILD'

$srv = stream_socket_server('tcp://127.0.0.1:0');
$addr = stream_socket_get_name($srv, false);
$serve = function (float $delay) use ($srv) {         // one slow HTTP response
    $c = stream_socket_accept($srv, 5);
    fread($c, 8192);
    usleep((int) ($delay * 1e6));
    fwrite($c, "HTTP/1.0 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello");
    fclose($c);
};

// A curl-multi loop, the way Guzzle's multi handler drives it.
$ticks = 0; $done = false; $body = null; $selects = [];
(new Loop)->runAll(
    function () use ($addr, &$done, &$body, &$selects) {
        $mh = curl_multi_init();
        $ch = curl_init("http://$addr/");
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_multi_add_handle($mh, $ch);
        do {
            curl_multi_exec($mh, $running);
            if ($running) $selects[] = curl_multi_select($mh, 1.0);
        } while ($running);
        $body = curl_multi_getcontent($ch);
        curl_multi_remove_handle($mh, $ch);
        $done = true;
    },
    fn() => $serve(0.3),
    function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } },
);
echo 'curl multi: ', $body, ', ', $ticks >= 5 ? 'cooperative' : "BLOCKED (ticks=$ticks)", "\n";
var_dump(min($selects) >= 0);                        // activity counts, never an error

// A select that times out returns 0 after its timeout, without blocking the others.
$mh = curl_multi_init();
$ch = curl_init("http://$addr/");
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_multi_add_handle($mh, $ch);
curl_multi_exec($mh, $running);
$ticks = 0; $done = false; $r = null;
(new Loop)->runAll(
    function () use ($mh, &$r, &$done) {
        $t = microtime(true);
        do { $n = curl_multi_select($mh, 0.2); curl_multi_exec($mh, $running); } while ($n > 0 && microtime(true) - $t < 0.2);
        $r = [$n, microtime(true) - $t >= 0.15];
        $done = true;
    },
    function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } },
);
echo 'timeout: ', json_encode($r), ', ', $ticks >= 3 ? 'cooperative' : "BLOCKED (ticks=$ticks)", "\n";
CHILD);
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' ' . escapeshellarg($child) . ' 2>&1');
unlink($child);
?>
--EXPECT--
curl multi: hello, cooperative
bool(true)
timeout: [0,true], cooperative
