--TEST--
tcp_server(): connect/data/half-close/close frames both ways, split writes, stale ids, bad frames, fclose
--EXTENSIONS--
phasync
--FILE--
<?php
require __DIR__ . '/tcp_server.inc';

$fp = \phasync\ext\tcp_server('127.0.0.1', 0);
stream_set_blocking($fp, false);
$name = stream_socket_get_name($fp, false);
var_dump((bool) preg_match('/^127\.0\.0\.1:\d+$/', $name), stream_socket_get_name($fp, true));

// C carries "peer local"; D both ways.
$a = stream_socket_client("tcp://$name");
[[$t, $id, $p]] = collect($fp, has('C'));
var_dump($t, $id, $p === stream_socket_get_name($a, false) . ' ' . $name);
fwrite($a, 'hello');
show(collect($fp, has('D')));
fwrite($fp, frame('D', 1, 'world'));
echo fread($a, 100), "\n";

// A frame written a byte at a time is reassembled.
foreach (str_split(frame('D', 1, 'split') . frame('D', 1, '!')) as $byte) fwrite($fp, $byte);
usleep(50000);
echo fread($a, 100), "\n";

// The client half-closes: E, and we can still write; then our E ends it: X 0.
stream_socket_shutdown($a, STREAM_SHUT_WR);
show(collect($fp, has('E')));
fwrite($fp, frame('D', 1, 'still here') . frame('E', 1));
echo fread($a, 100), "\n";
var_dump(fread($a, 100), feof($a));
show(collect($fp, has('X')));

// We close a second client: it sees EOF, and X answers the close.
$b = stream_socket_client("tcp://$name");
collect($fp, has('C', 2));
fwrite($fp, frame('D', 2, 'bye') . frame('X', 2));
echo stream_get_contents($b), "\n";
show(collect($fp, has('X', 2)));

// Frames for a connection that is gone are dropped; a bad type is an error.
var_dump(fwrite($fp, frame('D', 2, 'late') . frame('X', 99)));
var_dump(fwrite($fp, frame('Q', 1)));

// fclose() closes every connection.
$c = stream_socket_client("tcp://$name");
collect($fp, has('C', 3));
fclose($fp);
var_dump(stream_get_contents($c), feof($c));
var_dump(@stream_socket_client("tcp://$name", $errno, $errstr, 1));
?>
--EXPECTF--
bool(true)
bool(false)
string(1) "C"
int(1)
bool(true)
D 1 "hello"
world
split!
E 1 ""
still here
string(0) ""
bool(true)
X 1 "\u0000\u0000\u0000\u0000"
bye
X 2 "\u0000\u0000\u0000\u0000"
int(22)
%AWarning: fwrite(): Invalid frame type 0x51 in %s on line %d
bool(false)
string(0) ""
bool(true)
bool(false)
