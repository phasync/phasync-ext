/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: a8b1558ce74c81e8b230d652eaf0f6fd2ecad0fb */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_MASK_EX(arginfo_phasync_ext_stream_select, 0, 4, MAY_BE_LONG|MAY_BE_FALSE)
	ZEND_ARG_TYPE_INFO(1, read, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(1, write, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(1, except, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(0, seconds, IS_LONG, 1)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, microseconds, IS_LONG, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_phasync_ext_manage, 0, 6, IS_MIXED, 0)
	ZEND_ARG_OBJ_INFO(0, task, Closure, 0)
	ZEND_ARG_OBJ_INFO(0, getSlot, Closure, 0)
	ZEND_ARG_OBJ_INFO(0, park, Closure, 0)
	ZEND_ARG_OBJ_INFO(0, unpark, Closure, 0)
	ZEND_ARG_OBJ_INFO(0, sleep, Closure, 0)
	ZEND_ARG_TYPE_INFO(0, timeoutException, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_phasync_ext_poll, 0, 1, IS_VOID, 0)
	ZEND_ARG_TYPE_INFO(0, maxTime, IS_DOUBLE, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_phasync_ext_readable, 0, 1, IS_VOID, 0)
	ZEND_ARG_INFO(0, stream)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, timeout, IS_DOUBLE, 1, "null")
ZEND_END_ARG_INFO()

#define arginfo_phasync_ext_writable arginfo_phasync_ext_readable

ZEND_FUNCTION(phasync_ext_stream_select);
ZEND_FUNCTION(phasync_ext_manage);
ZEND_FUNCTION(phasync_ext_poll);
ZEND_FUNCTION(phasync_ext_readable);
ZEND_FUNCTION(phasync_ext_writable);

static const zend_function_entry ext_functions[] = {
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "stream_select"), zif_phasync_ext_stream_select, arginfo_phasync_ext_stream_select, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "manage"), zif_phasync_ext_manage, arginfo_phasync_ext_manage, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "poll"), zif_phasync_ext_poll, arginfo_phasync_ext_poll, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "readable"), zif_phasync_ext_readable, arginfo_phasync_ext_readable, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "writable"), zif_phasync_ext_writable, arginfo_phasync_ext_writable, 0, NULL, NULL)
	ZEND_FE_END
};
