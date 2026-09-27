<?php
// Frame helpers shared by the tcp_server tests.
function frame(string $type, int $id, string $payload = ''): string {
    return $type . pack('VV', $id, strlen($payload)) . $payload;
}
/** Parse a buffer that must hold only whole frames. */
function parse(string $buf): array {
    $out = []; $o = 0;
    while ($o < strlen($buf)) {
        if (strlen($buf) - $o < 9) throw new Exception('partial header');
        $h = unpack('atype/Vid/Vlen', $buf, $o);
        if (strlen($buf) - $o - 9 < $h['len']) throw new Exception('partial payload');
        $out[] = [$h['type'], $h['id'], substr($buf, $o + 9, $h['len'])];
        $o += 9 + $h['len'];
    }
    return $out;
}
/** Read frames until $done(frames so far) is true or $timeout seconds pass. */
function collect($fp, Closure $done, int $len = 65536, float $timeout = 5): array {
    $frames = []; $deadline = microtime(true) + $timeout;
    while (!$done($frames) && microtime(true) < $deadline) {
        $r = [$fp]; $w = $e = null;
        if (stream_select($r, $w, $e, 0, 50000) && ($chunk = fread($fp, $len)) !== '') {
            array_push($frames, ...parse($chunk));
        }
    }
    return $frames;
}
function show(array $frames): void {
    foreach ($frames as [$t, $id, $p]) {
        echo $t, ' ', $id, ' ', strlen($p) > 40 ? strlen($p) . ' bytes' : json_encode($p), "\n";
    }
}
function has(string $type, ?int $id = null): Closure {
    return fn($f) => (bool) array_filter($f, fn($x) => $x[0] === $type && ($id === null || $x[1] === $id));
}
