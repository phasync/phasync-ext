/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: a48c52d2204e8f7c5652b2a23c15ffe2e9be1a15 */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_MASK_EX(arginfo_phasync_ext_stream_select, 0, 4, MAY_BE_LONG|MAY_BE_FALSE)
	ZEND_ARG_TYPE_INFO(1, read, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(1, write, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(1, except, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(0, seconds, IS_LONG, 1)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, microseconds, IS_LONG, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_phasync_ext_manage, 0, 4, IS_MIXED, 0)
	ZEND_ARG_OBJ_INFO(0, code, Closure, 0)
	ZEND_ARG_OBJ_INFO(0, readHandler, Closure, 0)
	ZEND_ARG_OBJ_INFO(0, writeHandler, Closure, 0)
	ZEND_ARG_OBJ_INFO(0, sleepHandler, Closure, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_phasync_ext_is_auto_managed, 0, 1, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, stream, IS_MIXED, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(phasync_ext_stream_select);
ZEND_FUNCTION(phasync_ext_manage);
ZEND_FUNCTION(phasync_ext_is_auto_managed);

static const zend_function_entry ext_functions[] = {
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "stream_select"), zif_phasync_ext_stream_select, arginfo_phasync_ext_stream_select, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "manage"), zif_phasync_ext_manage, arginfo_phasync_ext_manage, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "is_auto_managed"), zif_phasync_ext_is_auto_managed, arginfo_phasync_ext_is_auto_managed, 0, NULL, NULL)
	ZEND_FE_END
};
