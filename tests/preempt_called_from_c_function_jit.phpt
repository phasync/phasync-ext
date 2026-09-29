--TEST--
set_preempt_function(): not called in PHP code called from C or by the engine, nor in destructors or handlers (function JIT)
--EXTENSIONS--
phasync
session
--SKIPIF--
<?php if (!extension_loaded('Zend OPcache')) die('skip needs opcache'); ?>
--INI--
opcache.enable=1
opcache.enable_cli=1
opcache.jit=function
opcache.jit_buffer_size=64M
session.use_cookies=0
session.cache_limiter=
session.save_path=
--FILE--
<?php require __DIR__ . '/preempt_called_from_c.inc';
--EXPECT--
session read handler: calls: 0
plain PHP code: interrupted
usort comparator: calls: 0
array_map callback: calls: 0
output handler: calls: 0
error handler: calls: 0
error handler: calls: 0
destructor: calls: 0
__get: calls: 0
Iterator::current() in foreach: calls: 0
stream wrapper read: calls: 0
a function called from PHP code: interrupted
exception handler: calls: 0
shutdown function: calls: 0
