--TEST--
set_preempt_function(): not called in PHP code called from C or by the engine, nor in destructors or handlers (no JIT)
--EXTENSIONS--
phasync
session
--INI--
opcache.enable_cli=0
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
