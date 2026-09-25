--TEST--
mysqlnd (mysqli and PDO_mysql) queries suspend the fiber instead of blocking the process
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!getenv('PHASYNC_TEST_MYSQL')) die('skip set PHASYNC_TEST_MYSQL=host:port:user:password');
if (!function_exists('proc_open')) die('skip requires proc_open');
?>
--FILE--
<?php
// mysqlnd connects through the hooked tcp:// transport, so its socket reads are
// wrapped. It detaches the stream from the resource list (net_stream->res = NULL),
// so the read handler gets a dup() of the socket fd. Fiber A runs SELECT SLEEP(1)
// while fiber B ticks every 200 ms: B must keep ticking while A's query runs.
// Runs in a child with the normal ini, because mysqli/pdo_mysql are usually shared
// extensions that run-tests' `-n` cannot load.
[$host, $port, $user, $pass] = explode(':', getenv('PHASYNC_TEST_MYSQL'));
$child = tempnam(sys_get_temp_dir(), 'phasync_my_') . '.php';
file_put_contents($child, '<?php
final class T extends Exception {}
[$host, $port, $user, $pass] = ' . var_export([$host, (int) $port, $user, $pass], true) . ';
foreach (["mysqli", "pdo"] as $mode) {
    if ($mode === "mysqli" && !class_exists("mysqli")) { echo "mysqli: missing\n"; continue; }
    if ($mode === "pdo" && !in_array("mysql", class_exists("PDO") ? PDO::getAvailableDrivers() : [], true)) { echo "pdo: missing\n"; continue; }
    $log = []; $waits = 0;
    $read  = function ($s, $t) use (&$waits) { $waits++; Fiber::suspend(["r", $s]); };
    $write = function ($s, $t) { Fiber::suspend(["w", $s]); };
    $sleep = function ($us) { Fiber::suspend(["t", microtime(true) + $us / 1e6]); };
    \phasync\ext\manage(function () use (&$log, $mode, $host, $port, $user, $pass) {
        $fibers = [
            "A" => new Fiber(function () use (&$log, $mode, $host, $port, $user, $pass) {
                if ($mode === "pdo") {
                    $v = (new PDO("mysql:host=$host;port=$port", $user, $pass))->query("SELECT SLEEP(1), 42")->fetch(PDO::FETCH_NUM)[1];
                } else {
                    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
                    $v = (new mysqli($host, $user, $pass, "", $port))->query("SELECT SLEEP(1), 42")->fetch_row()[1];
                }
                $log[] = "A:$v";
            }),
            "B" => new Fiber(function () use (&$log) {
                for ($i = 0; $i < 5; $i++) { usleep(200000); $log[] = "B"; }
            }),
        ];
        $wait = [];
        foreach ($fibers as $k => $f) $wait[$k] = $f->start();
        while ($wait = array_filter($wait)) {
            $r = $w = []; $e = null; $now = microtime(true); $to = 1.0;
            foreach ($wait as $k => [$type, $x]) {
                if ($type === "r") $r[$k] = $x; elseif ($type === "w") $w[$k] = $x; else $to = min($to, max(0, $x - $now));
            }
            if ($r || $w) \phasync\ext\stream_select($r, $w, $e, 0, (int) ($to * 1e6)); else usleep((int) ($to * 1e6));
            $now = microtime(true);
            foreach ($wait as $k => [$type, $x]) {
                if (($type === "r" && isset($r[$k])) || ($type === "w" && isset($w[$k])) || ($type === "t" && $x <= $now)) {
                    $wait[$k] = $fibers[$k]->resume();
                }
            }
        }
    }, $read, $write, $sleep, T::class);
    $ticksBeforeA = array_search("A:42", $log, true);
    printf("%s: result=%s ticks-before-A=%s waits>=1=%s\n", $mode,
        in_array("A:42", $log, true) ? "42" : "missing",
        $ticksBeforeA >= 3 ? ">=3" : var_export($ticksBeforeA, true),
        $waits >= 1 ? "yes" : "no");
}
');
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' ' . escapeshellarg($child) . ' 2>&1');
unlink($child);
?>
--EXPECT--
mysqli: result=42 ticks-before-A=>=3 waits>=1=yes
pdo: result=42 ticks-before-A=>=3 waits>=1=yes
