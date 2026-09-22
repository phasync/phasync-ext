/* This is a generated file, edit phasync.stub.php instead.
 * Stub hash: b4ba6696a3d767ca50de390c43c9fe739428ae26 */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_MASK_EX(arginfo_phasync_stream_select, 0, 4, MAY_BE_LONG|MAY_BE_FALSE)
	ZEND_ARG_TYPE_INFO(1, read, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(1, write, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(1, except, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(0, seconds, IS_LONG, 1)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, microseconds, IS_LONG, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_phasync_register_read_handler, 0, 1, IS_VOID, 0)
	ZEND_ARG_TYPE_INFO(0, handler, IS_CALLABLE, 1)
ZEND_END_ARG_INFO()

#define arginfo_phasync_register_write_handler arginfo_phasync_register_read_handler

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_phasync_enable_hooks, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

#define arginfo_phasync_disable_hooks arginfo_phasync_enable_hooks

ZEND_FUNCTION(phasync_stream_select);
ZEND_FUNCTION(phasync_register_read_handler);
ZEND_FUNCTION(phasync_register_write_handler);
ZEND_FUNCTION(phasync_enable_hooks);
ZEND_FUNCTION(phasync_disable_hooks);

static const zend_function_entry ext_functions[] = {
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync", "stream_select"), zif_phasync_stream_select, arginfo_phasync_stream_select, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync", "register_read_handler"), zif_phasync_register_read_handler, arginfo_phasync_register_read_handler, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync", "register_write_handler"), zif_phasync_register_write_handler, arginfo_phasync_register_write_handler, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync", "enable_hooks"), zif_phasync_enable_hooks, arginfo_phasync_enable_hooks, 0, NULL, NULL)
	ZEND_RAW_FENTRY(ZEND_NS_NAME("phasync", "disable_hooks"), zif_phasync_disable_hooks, arginfo_phasync_disable_hooks, 0, NULL, NULL)
	ZEND_FE_END
};
