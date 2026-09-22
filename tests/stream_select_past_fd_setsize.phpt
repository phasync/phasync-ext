--TEST--
phasync\stream_select() works past the FD_SETSIZE ceiling
--EXTENSIONS--
phasync
--SKIPIF--
<?php
$lim = @shell_exec('bash -lc "ulimit -n" 2>/dev/null');
if ((int)$lim < 3000) die("skip needs ulimit -n >= 3000");
?>
--FILE--
<?php
$pairs = [];
for ($i = 0; $i < 1500; $i++) {
    $p = @stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
    if (!$p) break;
    $pairs[] = $p;
}
if (count($pairs) < 1100) die("skip could not open enough pairs");
$last = $pairs[count($pairs) - 1];
fwrite($last[0], "x");
$r = array_map(fn($p) => $p[1], $pairs); $w = $e = null;
var_dump(\phasync\stream_select($r, $w, $e, 2));
var_dump(count($r));
?>
--EXPECT--
int(1)
int(1)
