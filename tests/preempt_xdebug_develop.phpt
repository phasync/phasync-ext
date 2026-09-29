--TEST--
GH #31: an observed checkpoint() doesn't corrupt memory under Xdebug's develop mode
--EXTENSIONS--
phasync
--SKIPIF--
<?php
$so = ini_get('extension_dir') . '/xdebug.so';
if (!is_file($so)) die('skip xdebug not installed for this php');
?>
--INI--
zend_extension=xdebug.so
xdebug.mode=develop
xdebug.start_with_request=yes
opcache.enable_cli=0
--FILE--
<?php require __DIR__ . '/preempt_xdebug.inc'; ?>
--EXPECT--
preempted
result ok
done
