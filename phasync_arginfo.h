/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: a4e7bb3eb30d6fea77f2a2529724b03d57586cc1 */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_MASK_EX(arginfo_phasync_ext_stream_select, 0, 4, MAY_BE_LONG|MAY_BE_FALSE)
	ZEND_ARG_TYPE_INFO(1, read, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(1, write, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(1, except, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(0, seconds, IS_LONG, 1)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, microseconds, IS_LONG, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_phasync_ext_manage, 0, 4, IS_MIXED, 0)
	ZEND_ARG_OBJ_INFO(0, task, Closure, 0)
	ZEND_ARG_OBJ_INFO(0, poller, phasync\\ext\\Poller, 0)
	ZEND_ARG_OBJ_INFO(0, sleep, Closure, 0)
	ZEND_ARG_TYPE_INFO(0, timeoutException, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_phasync_ext_virtualize, 0, 2, IS_MIXED, 0)
	ZEND_ARG_OBJ_INFO(0, code, Closure, 0)
	ZEND_ARG_TYPE_INFO(0, sapi, IS_OBJECT, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_phasync_ext_Poller___construct, 0, 0, 3)
	ZEND_ARG_OBJ_INFO(0, getSlot, Closure, 0)
	ZEND_ARG_OBJ_INFO(0, park, Closure, 0)
	ZEND_ARG_OBJ_INFO(0, unpark, Closure, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_phasync_ext_Poller_poll, 0, 1, IS_VOID, 0)
	ZEND_ARG_TYPE_INFO(0, maxTime, IS_DOUBLE, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_phasync_ext_Poller_readable, 0, 1, IS_VOID, 0)
	ZEND_ARG_TYPE_INFO(0, stream, IS_MIXED, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, timeout, IS_DOUBLE, 0, "PHP_FLOAT_MAX")
ZEND_END_ARG_INFO()

#define arginfo_class_phasync_ext_Poller_writable arginfo_class_phasync_ext_Poller_readable

ZEND_FUNCTION(phasync_ext_stream_select);
ZEND_FUNCTION(phasync_ext_manage);
ZEND_FUNCTION(phasync_ext_virtualize);
ZEND_METHOD(phasync_ext_Poller, __construct);
ZEND_METHOD(phasync_ext_Poller, poll);
ZEND_METHOD(phasync_ext_Poller, readable);
ZEND_METHOD(phasync_ext_Poller, writable);

static const zend_function_entry ext_functions[] = {
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "stream_select"), zif_phasync_ext_stream_select, arginfo_phasync_ext_stream_select, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "manage"), zif_phasync_ext_manage, arginfo_phasync_ext_manage, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync\\ext", "virtualize"), zif_phasync_ext_virtualize, arginfo_phasync_ext_virtualize, 0, NULL, NULL)
	ZEND_FE_END
};

static const zend_function_entry class_phasync_ext_Poller_methods[] = {
	ZEND_ME(phasync_ext_Poller, __construct, arginfo_class_phasync_ext_Poller___construct, ZEND_ACC_PUBLIC)
	ZEND_ME(phasync_ext_Poller, poll, arginfo_class_phasync_ext_Poller_poll, ZEND_ACC_PUBLIC)
	ZEND_ME(phasync_ext_Poller, readable, arginfo_class_phasync_ext_Poller_readable, ZEND_ACC_PUBLIC)
	ZEND_ME(phasync_ext_Poller, writable, arginfo_class_phasync_ext_Poller_writable, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static zend_class_entry *register_class_phasync_ext_Poller(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "phasync\\ext", "Poller", class_phasync_ext_Poller_methods);
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, ZEND_ACC_FINAL|ZEND_ACC_NO_DYNAMIC_PROPERTIES|ZEND_ACC_NOT_SERIALIZABLE);

	return class_entry;
}
