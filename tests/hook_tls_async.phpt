--TEST--
Transparent async over TLS: fread on a hooked tls:// socket suspends a fiber
--EXTENSIONS--
phasync
--SKIPIF--
<?php
if (!class_exists('Fiber')) die('skip requires Fibers');
if (!extension_loaded('openssl')) die('skip requires openssl');
if (!function_exists('proc_open')) die('skip requires proc_open');
?>
--FILE--
<?php
// self-signed cert, generated in-process
$pk = openssl_pkey_new(['private_key_bits'=>2048, 'private_key_type'=>OPENSSL_KEYTYPE_RSA]);
$csr = openssl_csr_new(['commonName'=>'localhost'], $pk);
$crt = openssl_csr_sign($csr, null, $pk, 1);
openssl_x509_export($crt, $certPem);
openssl_pkey_export($pk, $keyPem);
$pemFile = tempnam(sys_get_temp_dir(), 'phpem');
file_put_contents($pemFile, $certPem.$keyPem);

// TLS echo-ish server in a subprocess (plain PHP, no extension needed)
$server = '
$ctx = stream_context_create(["ssl"=>["local_cert"=>' . var_export($pemFile, true) . ',"verify_peer"=>false]]);
$s = stream_socket_server("tls://127.0.0.1:0",$e,$es,STREAM_SERVER_BIND|STREAM_SERVER_LISTEN,$ctx);
$n = stream_socket_get_name($s,false); echo "PORT:".substr($n,strrpos($n,":")+1)."\n"; flush();
$c = @stream_socket_accept($s,5);
if($c){ usleep(250000); fwrite($c,"tlshello"); usleep(150000); fclose($c); }
';
$p = proc_open([PHP_BINARY, '-n', '-r', $server], [1=>['pipe','w'], 2=>['pipe','w']], $pipes);
$port = null;
while (($line = fgets($pipes[1])) !== false) {
    if (preg_match('/PORT:(\d+)/', $line, $m)) { $port = $m[1]; break; }
}

$ctx = stream_context_create(['ssl'=>['verify_peer'=>false,'verify_peer_name'=>false]]);

$suspended = \phasync\ext\manage(function () use ($port, $ctx) {
    $fiber = new Fiber(function() use ($port,$ctx){
        $c = @stream_socket_client("tls://127.0.0.1:$port",$e,$es,5,STREAM_CLIENT_CONNECT,$ctx);
        echo "tls fiber read: " . fread($c, 100) . "\n";
    });
    $sig = $fiber->start();
    $suspended = ($sig !== null);
    while (!$fiber->isTerminated()) {
        [$type,$fd] = $sig;
        $r=$w=$ex=null;
        if ($type==='read') $r=[$fd]; else $w=[$fd];
        \phasync\ext\stream_select($r,$w,$ex,5);
        $sig = $fiber->resume();
    }
    return $suspended;
},
fn($fd)=>Fiber::suspend(['read',$fd]),
fn($fd)=>Fiber::suspend(['write',$fd]),
fn($us)=>Fiber::suspend(['sleep',$us]));

var_dump($suspended);   // proves it went async, not blocking
foreach ($pipes as $pp) @fclose($pp);
proc_close($p);
@unlink($pemFile);
?>
--EXPECT--
tls fiber read: tlshello
bool(true)
