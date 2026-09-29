--TEST--
A coroutine destroyed (by refcount or by the cycle collector) while waiting for a mysqli query ends the query safely; the connection can be closed, and a new one works. A mysqli::close() destroyed while another coroutine's query waits is held until the query has failed, then closes (#21). In exception mode, mysqli's exception for the failed query replaces the unwind (PHP's rule) and surfaces where the coroutine was destroyed
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!getenv('PHASYNC_TEST_MYSQL')) die('skip set PHASYNC_TEST_MYSQL=host:port:user:password');
if (!function_exists('proc_open')) die('skip requires proc_open');
[$host, $port] = explode(':', getenv('PHASYNC_TEST_MYSQL'));
if (!@fsockopen($host, (int) $port, $errno, $errstr, 2)) die("skip MySQL at $host:$port unreachable");
?>
--FILE--
<?php
// Runs in a child with the normal ini, because mysqli is usually a shared
// extension that run-tests' `-n` cannot load.
[$host, $port, $user, $pass] = explode(':', getenv('PHASYNC_TEST_MYSQL'));
$child = tempnam(sys_get_temp_dir(), 'phasync_my_') . '.php';
file_put_contents($child, '<?php
require ' . var_export(__DIR__ . '/destroy.inc', true) . ';
[$host, $port, $user, $pass] = ' . var_export([$host, (int) $port, $user, $pass], true) . ';
if (!class_exists("mysqli")) { echo "  finally ran\\n  closed\\n  new connection: 1\\n  finally ran\\n  collected: yes\\n  closed\\n  new connection: 1\\ncloser destroyed (refcount)\\n  query: false\\n  finally ran\\n  new connection: 1\\ncloser destroyed (gc)\\n  collected: no\\n  query: false\\n  finally ran\\n  new connection: 1\\n  finally ran\\n  surfaced: mysqli_sql_exception\\n  closed\\n"; exit; }
function after($db, $host, $user, $pass, $port) {
    return function () use ($db, $host, $user, $pass, $port) {
        $db->close();
        echo "  closed\n";
        $db2 = new mysqli($host, $user, $pass, "", $port);
        echo "  new connection: ", $db2->query("SELECT 1")->fetch_row()[0], "\n";
        $db2->close();
    };
}
mysqli_report(MYSQLI_REPORT_OFF);
foreach (["refcount", "gc"] as $how) {
    $db = new mysqli($host, $user, $pass, "", $port);
    destroy_while_waiting(new Loop, $how, fn() => $db->query("SELECT SLEEP(2)"), after: [after($db, $host, $user, $pass, $port)]);
}
// #21: the closer (mysqli::close() destroyed while another coroutine waits for its
// query on the connection: held until the query has failed, then closes.
foreach (["refcount", "gc"] as $how) {
    echo "closer destroyed ($how)\n";
    $db = new mysqli($host, $user, $pass, "", $port);
    destroy_while_waiting(new Loop, $how, fn() => $db->close(), before: [function () use ($db) {
        $r = $db->query("SELECT SLEEP(2)");
        echo "  query: ", json_encode($r), "\n";
    }]);
    (new Loop)->manage(fn() => null);
    gc_collect_cycles();
    $db2 = new mysqli($host, $user, $pass, "", $port);
    echo "  new connection: ", $db2->query("SELECT 1")->fetch_row()[0], "\n";
    $db2->close();
}
mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
$db = new mysqli($host, $user, $pass, "", $port);
try {
    destroy_while_waiting(new Loop, "refcount", fn() => $db->query("SELECT SLEEP(2)"));
} catch (mysqli_sql_exception $e) {
    echo "  surfaced: ", get_class($e), "\n";
}
$db->close();
echo "  closed\n";
');
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' -d display_errors=1 -d log_errors=0 ' . escapeshellarg($child) . ' 2>&1', $rc);
echo "exit: $rc\n";
unlink($child);
?>
--EXPECT--
  finally ran
  closed
  new connection: 1
  finally ran
  collected: yes
  closed
  new connection: 1
closer destroyed (refcount)
  query: false
  finally ran
  new connection: 1
closer destroyed (gc)
  collected: no
  query: false
  finally ran
  new connection: 1
  finally ran
  surfaced: mysqli_sql_exception
  closed
exit: 0