--TEST--
sem_acquire()/msg_receive() wait cooperatively inside a scope (retrying the non-blocking forms); a cancelled wait takes nothing
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('proc_open')) die('skip requires proc_open');
// The body runs in a child PHP with the normal ini; skip if that has no sysvsem/sysvmsg.
if (trim((string) shell_exec(escapeshellarg(PHP_BINARY) . ' -r "echo function_exists(\'sem_acquire\') && function_exists(\'msg_receive\') ? 1 : 0;"')) !== '1') die('skip requires sysvsem and sysvmsg');
?>
--FILE--
<?php
$child = tempnam(sys_get_temp_dir(), 'phasync_ipc_') . '.php';
file_put_contents($child, '<?php require ' . var_export(__DIR__ . '/loop.inc', true) . ';' . <<<'CHILD'

function coop(string $name, Closure $op, Closure $helper): mixed {
    $ticks = 0; $done = false; $res = null;
    (new Loop)->runAll(
        function () use ($op, &$done, &$res) { $res = $op(); $done = true; },
        $helper,
        function () use (&$ticks, &$done) { while (!$done) { usleep(20000); $ticks++; } },
    );
    printf("%-14s %s\n", $name, $ticks >= 5 ? 'cooperative' : "BLOCKED (ticks=$ticks)");
    return $res;
}
$key = ftok(__FILE__, 'p') ^ getmypid();

// A semaphore held through one handle: acquiring it through another waits.
$s1 = sem_get($key, 1);
$s2 = sem_get($key, 1);
sem_acquire($s1);
var_dump(coop('sem_acquire', fn() => sem_acquire($s2), function () use ($s1) { usleep(200000); sem_release($s1); }));
sem_release($s2);

// A cancelled wait doesn't take the semaphore.
sem_acquire($s1);
$l = new Loop;
$l->runAll(
    function () use ($s2) {
        $GLOBALS['w'] = Fiber::getCurrent();
        try { sem_acquire($s2); echo "acquired?!\n"; } catch (LoopCancelled $e) { echo "sem wait cancelled\n"; }
    },
    function () use ($l) { usleep(50000); $l->cancel($GLOBALS['w']); },
);
sem_release($s1);
var_dump(sem_acquire($s2, true));                     // free: the cancelled wait took nothing
sem_release($s2);
sem_remove($s1);

// A message queue: msg_receive waits for the message, with native results.
$q = msg_get_queue($key);
$r = coop('msg_receive', function () use ($q) { $ok = msg_receive($q, 0, $type, 1024, $msg, true, 0, $err); return [$ok, $type, $msg, $err]; },
    function () use ($q) { usleep(200000); msg_send($q, 7, ['hello']); });
var_dump($r === [true, 7, ['hello'], 0]);

// A cancelled receive doesn't take the message.
$l = new Loop;
$l->runAll(
    function () use ($q) {
        $GLOBALS['w'] = Fiber::getCurrent();
        try { msg_receive($q, 0, $type, 1024, $msg); echo "received?!\n"; } catch (LoopCancelled $e) { echo "msg wait cancelled\n"; }
    },
    function () use ($l) { usleep(50000); $l->cancel($GLOBALS['w']); },
);
msg_send($q, 8, 'kept');
var_dump(msg_receive($q, 0, $type, 1024, $msg, true, MSG_IPC_NOWAIT), $type, $msg);
msg_remove_queue($q);
CHILD);
$so = realpath(ini_get('extension_dir') . '/phasync.so');
passthru(escapeshellarg(PHP_BINARY) . ' -d extension=' . escapeshellarg($so) . ' ' . escapeshellarg($child) . ' 2>&1');
unlink($child);
?>
--EXPECT--
sem_acquire    cooperative
bool(true)
sem wait cancelled
bool(true)
msg_receive    cooperative
bool(true)
msg wait cancelled
bool(true)
int(8)
string(4) "kept"
