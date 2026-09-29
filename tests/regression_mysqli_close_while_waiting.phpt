--TEST--
Regression: mysqli::close() by one coroutine while another waits for its query fails the query instead of crashing (#14)
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
// Coroutine A waits in mysqlnd's read of its query's result; B closes the
// connection (after a ping that fails: commands out of sync). mysqlnd frees the
// connection's stream in close; up to 0.5.0-alpha13 A then resumed on the freed
// stream and the process segfaulted. Runs in a child with the normal ini, because
// mysqli is usually a shared extension that run-tests' `-n` cannot load.
[$host, $port, $user, $pass] = explode(':', getenv('PHASYNC_TEST_MYSQL'));
$child = tempnam(sys_get_temp_dir(), 'phasync_my_') . '.php';
file_put_contents($child, '<?php
require ' . var_export(__DIR__ . '/loop.inc', true) . ';
[$host, $port, $user, $pass] = ' . var_export([$host, (int) $port, $user, $pass], true) . ';
if (!class_exists("mysqli")) { echo "a: failed\nb: closed\nb: closed\n"; exit; }
mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
foreach ([true, false] as $ping) {
    $db = new mysqli($host, $user, $pass, "", $port);
    (new Loop)->runAll(
        function () use ($db) {
            try { $db->query("SELECT SLEEP(1)"); echo "a: no error\n"; } catch (Throwable $e) { echo "a: failed\n"; }
        },
        function () use ($db, $ping) {
            usleep(50000);
            if ($ping) {
                try { $db->ping(); } catch (mysqli_sql_exception $e) { echo "b: ", $e->getMessage(), "\n"; }
            }
            $db->close();
            echo "b: closed\n";
        },
    );
}
');
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' -d display_errors=0 -d log_errors=0 ' . escapeshellarg($child) . ' 2>&1', $rc);
echo "exit: $rc\n";
unlink($child);
?>
--EXPECT--
b: Commands out of sync; you can't run this command now
a: failed
b: closed
a: failed
b: closed
exit: 0
