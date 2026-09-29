--TEST--
virtualize(): $_SESSION is per request, as the session it belongs to (#13)
--EXTENSIONS--
phasync
session
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--INI--
session.use_strict_mode=0
session.use_cookies=1
session.use_only_cookies=1
session.cache_limiter=nocache
session.serialize_handler=php
--FILE--
<?php
require __DIR__ . '/sink.inc';
use function phasync\ext\virtualize;

$dir = sys_get_temp_dir() . '/phasync_sessg_' . getmypid();
@mkdir($dir);
ini_set('session.save_path', $dir);
$saved = fn(string $id) => @file_get_contents("$dir/sess_$id");

// The worker's own $_SESSION, a plain global here: boundaries leave it alone.
$_SESSION = ['worker' => true];

// Runs each closure as a request in its own fiber, resuming them in turn until all end.
function interleave(array $requests): void {
    $fibers = [];
    foreach ($requests as $name => [$code, $sapi]) {
        $fibers[$name] = new Fiber(fn() => virtualize($code, $sapi));
        $fibers[$name]->start();
    }
    while ($fibers) {
        echo "worker: ", json_encode($_SESSION), "\n";
        foreach ($fibers as $name => $f) {
            if ($f->isTerminated()) unset($fibers[$name]); else $f->resume();
        }
    }
    foreach ($requests as [, $sapi]) echo $sapi->out;   // what each request echoed
}

echo "--- three requests, three sessions\n";
$req = function (string $v) {
    return function () use ($v) {
        session_start();
        $_SESSION['who'] = $v;
        Fiber::suspend();
        echo "$v sees ", json_encode($_SESSION), "\n";
        $_SESSION['more'] = "$v$v";
        Fiber::suspend();
        echo "$v sees ", json_encode($_SESSION), "\n";
        session_write_close();
    };
};
interleave([
    'a' => [$req('a'), Sink::withSession('sa')],
    'b' => [$req('b'), Sink::withSession('sb')],
    'c' => [$req('c'), Sink::withSession('sc')],
]);
var_dump($saved('sa'), $saved('sb'), $saved('sc'));

echo "--- a request without a session, beside one with\n";
interleave([
    'with' => [function () {
        session_start();
        $_SESSION['who'] = 'with';
        Fiber::suspend();
        echo "with sees ", json_encode($_SESSION), "\n";
    }, Sink::withSession('sw')],
    'without' => [function () {
        Fiber::suspend();
        var_dump(isset($_SESSION), array_key_exists('_SESSION', $GLOBALS));
        $_SESSION['mine'] = 'plain array';   // no session: an ordinary array of its own
        Fiber::suspend();
        echo "without sees ", json_encode($_SESSION), "\n";
    }, new Sink],
]);
var_dump($saved('sw'));

echo "--- session_regenerate_id(), session_destroy(), unset(), = []\n";
interleave([
    'regen' => [function () {
        session_start();
        $_SESSION['r'] = 1;
        Fiber::suspend();
        session_regenerate_id(true);
        $GLOBALS['regenerated'] = session_id();
        Fiber::suspend();
        echo "regen sees ", json_encode($_SESSION), "\n";
    }, Sink::withSession('sr')],
    'destroy' => [function () {
        session_start();
        $_SESSION['d'] = 1;
        Fiber::suspend();
        session_destroy();
        Fiber::suspend();
        echo "destroy sees ", json_encode($_SESSION), " status ", session_status(), "\n";
    }, Sink::withSession('sd')],
    'unset' => [function () {
        session_start();
        $_SESSION['u'] = 1;
        Fiber::suspend();
        unset($_SESSION);
        Fiber::suspend();
        var_dump(isset($_SESSION));
    }, Sink::withSession('su')],
    'empty' => [function () {
        session_start();
        $_SESSION['old'] = 1;
        Fiber::suspend();
        $_SESSION = [];
        Fiber::suspend();
        $_SESSION['new'] = 1;
        echo "empty sees ", json_encode($_SESSION), "\n";
    }, Sink::withSession('se')],
]);
var_dump($saved('sr'), $saved($regenerated), $saved('sd'), $saved('su'), $saved('se'));

echo "--- references into \$_SESSION, and fibers inside a request\n";
interleave([
    'ref' => [function () {
        session_start();
        $k = &$_SESSION['k'];
        $all = &$_SESSION;
        Fiber::suspend();
        $k = 'ref';
        $all['all'] = 'ref';
        Fiber::suspend();
        echo "ref sees ", json_encode($_SESSION), "\n";
    }, Sink::withSession('sref')],
    'nested' => [function () {
        session_start();
        $_SESSION['k'] = 'nested';
        $child = new Fiber(function () {
            $_SESSION['child'] = 'set';
            Fiber::suspend();
            echo "child sees ", json_encode($_SESSION), "\n";
            $_SESSION['child'] = 'again';
        });
        $child->start();
        Fiber::suspend();
        $child->resume();
        Fiber::suspend();
        echo "nested sees ", json_encode($_SESSION), "\n";
    }, Sink::withSession('snest')],
]);
var_dump($saved('sref'), $saved('snest'));

echo "--- the worker's\n";
var_dump($_SESSION, session_status() === PHP_SESSION_NONE);

array_map('unlink', glob("$dir/sess_*"));
rmdir($dir);
?>
--EXPECT--
--- three requests, three sessions
worker: {"worker":true}
worker: {"worker":true}
worker: {"worker":true}
a sees {"who":"a"}
a sees {"who":"a","more":"aa"}
b sees {"who":"b"}
b sees {"who":"b","more":"bb"}
c sees {"who":"c"}
c sees {"who":"c","more":"cc"}
string(26) "who|s:1:"a";more|s:2:"aa";"
string(26) "who|s:1:"b";more|s:2:"bb";"
string(26) "who|s:1:"c";more|s:2:"cc";"
--- a request without a session, beside one with
worker: {"worker":true}
worker: {"worker":true}
worker: {"worker":true}
with sees {"who":"with"}
bool(false)
bool(false)
without sees {"mine":"plain array"}
string(15) "who|s:4:"with";"
--- session_regenerate_id(), session_destroy(), unset(), = []
worker: {"worker":true}
worker: {"worker":true}
worker: {"worker":true}
regen sees {"r":1}
destroy sees {"d":1} status 1
bool(false)
empty sees {"new":1}
bool(false)
string(6) "r|i:1;"
bool(false)
string(6) "u|i:1;"
string(8) "new|i:1;"
--- references into $_SESSION, and fibers inside a request
worker: {"worker":true}
worker: {"worker":true}
worker: {"worker":true}
ref sees {"k":"ref","all":"ref"}
child sees {"k":"nested","child":"set"}
nested sees {"k":"nested","child":"again"}
string(26) "k|s:3:"ref";all|s:3:"ref";"
string(33) "k|s:6:"nested";child|s:5:"again";"
--- the worker's
array(1) {
  ["worker"]=>
  bool(true)
}
bool(true)
