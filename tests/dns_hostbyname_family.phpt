--TEST--
gethostbyname()/gethostbynamel()/gethostbyaddr() run on the pool inside a scope and match native results and warnings
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--FILE--
<?php
final class TestTimeout extends Exception {}
$waits = 0;
$rd = function ($s, $t) use (&$waits) {                       // waits like phasync does
    $waits++;
    $r = [$s]; $w = $e = null;
    if (\phasync\ext\stream_select($r, $w, $e, 5) < 1) throw new TestTimeout();
};
function call(callable $fn, string $arg): string {
    error_clear_last();
    $v = @$fn($arg);
    $w = error_get_last()['message'] ?? '';
    return json_encode($v) . ($w !== '' ? ' + warning: ' . preg_replace('/^.*?\(\): /', '', $w) : '');
}
$cases = [
    ['gethostbyname', 'localhost'], ['gethostbyname', '127.0.0.1'], ['gethostbyname', 'no-such-host.invalid'],
    ['gethostbyname', str_repeat('a', 300)],
    ['gethostbynamel', 'localhost'], ['gethostbynamel', 'no-such-host.invalid'], ['gethostbynamel', str_repeat('a', 300)],
    ['gethostbyaddr', '127.0.0.1'], ['gethostbyaddr', '::1'], ['gethostbyaddr', 'not-an-ip'],
];
foreach ($cases as [$fn, $arg]) {
    $native = call($fn, $arg);
    $ext = null;
    $body = function () use ($fn, $arg, &$ext) { $ext = call($fn, $arg); };   // outside the arrow fn
    (new Fiber(fn() => \phasync\ext\manage($body, $rd, $rd, fn($us) => null, TestTimeout::class)))->start();
    printf("%-15s %-24s %s\n", $fn, strlen($arg) > 24 ? 'a*' . strlen($arg) : $arg, $native === $ext ? 'same' : "DIFF native=$native ext=$ext");
}
var_dump($waits >= 6);            // the lookups went through the pool (read handler)
?>
--EXPECT--
gethostbyname   localhost                same
gethostbyname   127.0.0.1                same
gethostbyname   no-such-host.invalid     same
gethostbyname   a*300                    same
gethostbynamel  localhost                same
gethostbynamel  no-such-host.invalid     same
gethostbynamel  a*300                    same
gethostbyaddr   127.0.0.1                same
gethostbyaddr   ::1                      same
gethostbyaddr   not-an-ip                same
bool(true)
