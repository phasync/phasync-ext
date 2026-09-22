--TEST--
phasync\stream_select() basic readiness
--EXTENSIONS--
phasync
--FILE--
<?php
[$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
fwrite($b, "hi");
$r = [$a]; $w = $e = null;
var_dump(\phasync\stream_select($r, $w, $e, 1));
var_dump(count($r));
var_dump(fread($r[0], 2));
?>
--EXPECT--
int(1)
int(1)
string(2) "hi"
