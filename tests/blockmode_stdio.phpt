--TEST--
STDOUT/STDERR's real O_NONBLOCK flag is unchanged by a cooperative write inside a scope
(#32; these are wrapped RAW at RINIT and may be shared with the test harness)
--EXTENSIONS--
phasync
--SKIPIF--
<?php if (!is_dir('/proc/self/fd')) die('skip requires /proc'); ?>
--FILE--
<?php
require __DIR__ . '/loop.inc';
require __DIR__ . '/fdflags.inc';

$outBefore = real_nonblock(1);
$errBefore = real_nonblock(2);

$loop = new Loop;
$loop->runAll(function () {
    fwrite(STDOUT, "out\n");
    fwrite(STDERR, "err\n");   // not part of stdout comparison
});

var_dump(real_nonblock(1) === $outBefore);
var_dump(real_nonblock(2) === $errBefore);
?>
--EXPECT--
out
err
bool(true)
bool(true)
