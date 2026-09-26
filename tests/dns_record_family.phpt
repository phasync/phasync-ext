--TEST--
dns_get_record()/getmxrr()/checkdnsrr() query on the pool inside a scope and match native results and errors
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!function_exists('dns_get_record')) die('skip no DNS functions');
if (!@checkdnsrr('php.net', 'A')) die('skip requires working DNS');
?>
--FILE--
<?php
final class TestTimeout extends Exception {}
$waits = 0;
$rd = function ($s, $t) use (&$waits) {                        // waits like phasync does
    $waits++;
    $r = [$s]; $w = $e = null;
    if (\phasync\ext\stream_select($r, $w, $e, 10) < 1) throw new TestTimeout();
};
// Two live queries can differ in TTL and round-robin order: zero TTLs, sort.
function norm($v) {
    if (!is_array($v)) return $v;
    $v = array_map('norm', $v);
    if (array_key_exists('ttl', $v)) $v['ttl'] = 0;
    if (array_is_list($v)) { $v = array_map('json_encode', $v); sort($v); }
    return $v;
}
function run(Closure $f): string {
    error_clear_last();
    try { $r = $f(); } catch (Throwable $e) { return get_class($e) . ': ' . $e->getMessage(); }
    $w = error_get_last()['message'] ?? '';
    return json_encode(norm($r)) . ($w !== '' ? ' + warning: ' . preg_replace('/^.*?\(\): /', '', $w) : '');
}
$cases = [
    'A php.net'          => fn() => dns_get_record('php.net', DNS_A),
    'AAAA localhost'     => fn() => dns_get_record('localhost', DNS_AAAA),
    'MX+NS+TXT php.net'  => fn() => dns_get_record('php.net', DNS_MX | DNS_NS | DNS_TXT),
    'SOA php.net'        => fn() => dns_get_record('php.net', DNS_SOA),
    'ANY localhost'      => fn() => dns_get_record('localhost'),
    'raw A php.net'      => fn() => dns_get_record('php.net', 1, $a, $b, true),
    'NS authns/addtl'    => function () { $r = dns_get_record('php.net', DNS_NS, $auth, $add); return [$r, $auth, $add]; },
    'NXDOMAIN'           => fn() => dns_get_record('no-such-host.invalid', DNS_A),
    'bad type'           => fn() => dns_get_record('php.net', 0x40000000),
    'getmxrr php.net'    => function () { $ok = getmxrr('php.net', $mx, $w); return [$ok, $mx, $w]; },
    'getmxrr NXDOMAIN'   => function () { $ok = getmxrr('no-such-host.invalid', $mx); return [$ok, $mx]; },
    'checkdnsrr MX'      => fn() => checkdnsrr('php.net', 'MX'),
    'checkdnsrr NX'      => fn() => checkdnsrr('no-such-host.invalid', 'A'),
    'checkdnsrr empty'   => fn() => checkdnsrr(''),
    'checkdnsrr badtype' => fn() => checkdnsrr('php.net', 'NOPE'),
    'dns_check_record'   => fn() => dns_check_record('php.net', 'A'),
    'dns_get_mx'         => function () { $ok = dns_get_mx('php.net', $mx); return [$ok, count($mx) > 0]; },
];
foreach ($cases as $name => $f) {
    $native = run($f);
    $ext = null;
    $body = function () use ($f, &$ext) { $ext = run($f); };    // outside the arrow fn
    (new Fiber(fn() => \phasync\ext\manage($body, $rd, $rd, fn($us) => null, TestTimeout::class)))->start();
    printf("%-20s %s\n", $name, $native === $ext ? 'same' : "DIFF\n  native=$native\n  ext   =$ext");
}
var_dump($waits >= 12);                  // the queries went through the pool
?>
--EXPECT--
A php.net            same
AAAA localhost       same
MX+NS+TXT php.net    same
SOA php.net          same
ANY localhost        same
raw A php.net        same
NS authns/addtl      same
NXDOMAIN             same
bad type             same
getmxrr php.net      same
getmxrr NXDOMAIN     same
checkdnsrr MX        same
checkdnsrr NX        same
checkdnsrr empty     same
checkdnsrr badtype   same
dns_check_record     same
dns_get_mx           same
bool(true)
