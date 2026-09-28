--TEST--
virtualize(): a fatal error inside is displayed in the request's output and ends the worker through its own shutdown, cleanly
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!class_exists('Fiber')) die('skip requires Fibers'); ?>
--INI--
display_errors=1
log_errors=0
--FILE--
<?php
class Sink {
    public function ub_write(string $d): bool { fwrite(STDOUT, "[request] $d"); return true; }
    public function send_headers(int $s, ?string $l, array $h): void {}
}
register_shutdown_function(function () { echo "worker shutdown\n"; });
(new Fiber(fn() => phasync\ext\virtualize(function () {
    echo "in the request\n";
    eval('class Twice {} class Twice {}');
}, new Sink)))->start();
echo "never\n";
?>
--EXPECTF--
[request] in the request
[request] %A
Fatal error: Cannot %s class Twice%s
worker shutdown
