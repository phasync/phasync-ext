/* phasync extension
 *
 * Tier 1: phasync\stream_select() — growable, poll(2)-based, no FD_SETSIZE limit.
 *         Accepts stream resources and plain integer file descriptors.
 *
 * Tier 2: transparent async I/O for sockets. enable_hooks() re-registers the
 *         tcp:// and unix:// transports; sockets created afterwards get their
 *         reads/writes wrapped. On a would-block (EAGAIN) the wrapped op invokes
 *         the userland handler registered via register_read_handler()/
 *         register_write_handler() with the integer fd. The handler waits until
 *         the fd is ready (typically Fiber::suspend into a scheduler) and returns;
 *         the extension then performs the actual read/write. The C side never
 *         touches the fiber API — the userland callback owns all suspension.
 */
#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include "php.h"
#include "php_streams.h"
#include "php_network.h"
#include "zend_exceptions.h"
#include "phasync_arginfo.h"

#include <poll.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>

ZEND_BEGIN_MODULE_GLOBALS(phasync)
	zval read_handler;
	zval write_handler;
	php_stream_transport_factory orig_tcp;
	php_stream_transport_factory orig_unix;
	const php_stream_ops *orig_ops;   /* the socket ops we wrap (captured lazily) */
	php_stream_ops wrapped_ops;       /* copy of orig_ops with read/write/close overridden */
	bool wrapped_ready;
	bool hooks_enabled;
	HashTable hooked;                 /* (uintptr_t)stream -> saved fd flags (as long) */
ZEND_END_MODULE_GLOBALS(phasync)

ZEND_DECLARE_MODULE_GLOBALS(phasync)

#ifdef ZTS
# define PHASYNC_G(v) ZEND_MODULE_GLOBALS_ACCESSOR(phasync, v)
#else
# define PHASYNC_G(v) (phasync_globals.v)
#endif

/* ---- fd helpers ---------------------------------------------------------- */

static php_socket_t phasync_stream_fd(php_stream *stream)
{
	php_socket_t fd = -1;
	php_stream_cast(stream, PHP_STREAM_AS_FD_FOR_SELECT | PHP_STREAM_CAST_INTERNAL,
		(void *) &fd, 0);
	return fd;
}

/* Call the userland wait handler with the fd; returns -1 if it threw. */
static int phasync_call_wait(zval *handler, php_socket_t fd)
{
	zval args[1], retval;
	int rc = 0;

	ZVAL_LONG(&args[0], (zend_long) fd);
	ZVAL_UNDEF(&retval);
	if (call_user_function(NULL, NULL, handler, &retval, 1, args) == FAILURE || EG(exception)) {
		rc = -1;
	}
	zval_ptr_dtor(&retval);
	return rc;
}

static void phasync_ensure_nonblocking(php_stream *stream, php_socket_t fd);

/* ---- wrapped socket ops -------------------------------------------------- */

static ssize_t phasync_wrapped_read(php_stream *stream, char *buf, size_t count)
{
	php_socket_t fd = phasync_stream_fd(stream);
	zval *handler = &PHASYNC_G(read_handler);

	if (fd == -1 || Z_ISUNDEF_P(handler)) {
		return PHASYNC_G(orig_ops)->read(stream, buf, count);
	}
	phasync_ensure_nonblocking(stream, fd);
	for (;;) {
		ssize_t n = recv(fd, buf, count, 0);
		if (n > 0) {
			return n;
		}
		if (n == 0) {
			stream->eof = 1;   /* orderly shutdown */
			return 0;
		}
		if (errno == EINTR) {
			continue;
		}
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			stream->eof = 1;
			return -1;
		}
		/* would block: let userland wait (may suspend the fiber), then retry */
		if (phasync_call_wait(handler, fd) != 0) {
			return -1;
		}
	}
}

static ssize_t phasync_wrapped_write(php_stream *stream, const char *buf, size_t count)
{
	php_socket_t fd = phasync_stream_fd(stream);
	zval *handler = &PHASYNC_G(write_handler);

	if (fd == -1 || Z_ISUNDEF_P(handler)) {
		return PHASYNC_G(orig_ops)->write(stream, buf, count);
	}
	phasync_ensure_nonblocking(stream, fd);
	for (;;) {
		ssize_t n = send(fd, buf, count, 0);
		if (n >= 0) {
			return n;
		}
		if (errno == EINTR) {
			continue;
		}
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			return -1;
		}
		if (phasync_call_wait(&PHASYNC_G(write_handler), fd) != 0) {
			return -1;
		}
	}
}

static int phasync_wrapped_close(php_stream *stream, int close_handle)
{
	zval *saved = zend_hash_index_find(&PHASYNC_G(hooked), (zend_ulong) (uintptr_t) stream);
	if (saved) {
		php_socket_t fd = phasync_stream_fd(stream);
		if (fd != -1) {
			fcntl(fd, F_SETFL, (int) Z_LVAL_P(saved));  /* restore original flags */
		}
		zend_hash_index_del(&PHASYNC_G(hooked), (zend_ulong) (uintptr_t) stream);
	}
	return PHASYNC_G(orig_ops)->close(stream, close_handle);
}

static void phasync_wrap_stream(php_stream *stream)
{
	if (stream == NULL) {
		return;
	}
	/* Capture the socket ops once and build the wrapped copy. */
	if (!PHASYNC_G(wrapped_ready)) {
		PHASYNC_G(orig_ops) = stream->ops;
		PHASYNC_G(wrapped_ops) = *stream->ops;
		PHASYNC_G(wrapped_ops).read = phasync_wrapped_read;
		PHASYNC_G(wrapped_ops).write = phasync_wrapped_write;
		PHASYNC_G(wrapped_ops).close = phasync_wrapped_close;
		PHASYNC_G(wrapped_ops).label = "phasync-wrapped-socket";
		PHASYNC_G(wrapped_ready) = 1;
	}
	/* Only wrap streams using the exact ops we captured (plain tcp/unix sockets). */
	if (stream->ops != PHASYNC_G(orig_ops)) {
		return;
	}
	/* Replace the ops now; defer fd acquisition + non-blocking setup to the first
	 * I/O, since the stream is not reliably castable to an fd at factory time. */
	stream->ops = &PHASYNC_G(wrapped_ops);
}

/* Lazily make the underlying fd non-blocking on first I/O, saving prior flags. */
static void phasync_ensure_nonblocking(php_stream *stream, php_socket_t fd)
{
	int flags;

	if (fd == -1
	 || zend_hash_index_exists(&PHASYNC_G(hooked), (zend_ulong) (uintptr_t) stream)) {
		return;
	}
	flags = fcntl(fd, F_GETFL, 0);
	if (flags != -1) {
		zval z;
		ZVAL_LONG(&z, flags);
		zend_hash_index_update(&PHASYNC_G(hooked), (zend_ulong) (uintptr_t) stream, &z);
		fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	}
}

/* ---- transport factories ------------------------------------------------- */

static php_stream *phasync_tcp_factory(const char *proto, size_t protolen,
		const char *resourcename, size_t resourcenamelen, const char *persistent_id,
		int options, int flags, struct timeval *timeout,
		php_stream_context *context STREAMS_DC)
{
	php_stream *s = PHASYNC_G(orig_tcp)(proto, protolen, resourcename, resourcenamelen,
		persistent_id, options, flags, timeout, context STREAMS_CC);
	phasync_wrap_stream(s);
	return s;
}

static php_stream *phasync_unix_factory(const char *proto, size_t protolen,
		const char *resourcename, size_t resourcenamelen, const char *persistent_id,
		int options, int flags, struct timeval *timeout,
		php_stream_context *context STREAMS_DC)
{
	php_stream *s = PHASYNC_G(orig_unix)(proto, protolen, resourcename, resourcenamelen,
		persistent_id, options, flags, timeout, context STREAMS_CC);
	phasync_wrap_stream(s);
	return s;
}

/* ---- phasync\enable_hooks / disable_hooks -------------------------------- */

ZEND_FUNCTION(phasync_enable_hooks)
{
	HashTable *xhash;

	ZEND_PARSE_PARAMETERS_NONE();

	if (PHASYNC_G(hooks_enabled)) {
		return;
	}
	xhash = php_stream_xport_get_hash();
	PHASYNC_G(orig_tcp)  = zend_hash_str_find_ptr(xhash, "tcp", sizeof("tcp") - 1);
	PHASYNC_G(orig_unix) = zend_hash_str_find_ptr(xhash, "unix", sizeof("unix") - 1);

	if (PHASYNC_G(orig_tcp)) {
		php_stream_xport_register("tcp", phasync_tcp_factory);
	}
	if (PHASYNC_G(orig_unix)) {
		php_stream_xport_register("unix", phasync_unix_factory);
	}
	PHASYNC_G(hooks_enabled) = 1;
}

ZEND_FUNCTION(phasync_disable_hooks)
{
	ZEND_PARSE_PARAMETERS_NONE();

	if (!PHASYNC_G(hooks_enabled)) {
		return;
	}
	if (PHASYNC_G(orig_tcp)) {
		php_stream_xport_register("tcp", PHASYNC_G(orig_tcp));
	}
	if (PHASYNC_G(orig_unix)) {
		php_stream_xport_register("unix", PHASYNC_G(orig_unix));
	}
	PHASYNC_G(hooks_enabled) = 0;
}

/* ---- phasync\register_read_handler / register_write_handler --------------- */

static void phasync_set_handler(zval *slot, INTERNAL_FUNCTION_PARAMETERS)
{
	zend_fcall_info fci;
	zend_fcall_info_cache fcc;

	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_FUNC_OR_NULL(fci, fcc)
	ZEND_PARSE_PARAMETERS_END();

	zval_ptr_dtor(slot);
	if (ZEND_FCI_INITIALIZED(fci)) {
		ZVAL_COPY(slot, &fci.function_name);
	} else {
		ZVAL_UNDEF(slot);
	}
}

ZEND_FUNCTION(phasync_register_read_handler)
{
	phasync_set_handler(&PHASYNC_G(read_handler), INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

ZEND_FUNCTION(phasync_register_write_handler)
{
	phasync_set_handler(&PHASYNC_G(write_handler), INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

/* ---- phasync\stream_select (Tier 1) -------------------------------------- */

/* Extract an fd from an array element: a stream resource or a plain integer. */
static php_socket_t phasync_elem_fd(zval *elem)
{
	php_stream *stream;

	ZVAL_DEREF(elem);
	if (Z_TYPE_P(elem) == IS_LONG) {
		return (php_socket_t) Z_LVAL_P(elem);
	}
	php_stream_from_zval_no_verify(stream, elem);
	if (stream == NULL) {
		return -1;
	}
	return phasync_stream_fd(stream);
}

static int phasync_collect(zval *array, HashTable *events_by_fd, short want)
{
	zval *elem;
	int cnt = 0;

	if (!array || Z_TYPE_P(array) != IS_ARRAY) {
		return 0;
	}
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(array), elem) {
		php_socket_t fd = phasync_elem_fd(elem);
		if (fd != -1) {
			zval *cur = zend_hash_index_find(events_by_fd, (zend_ulong) fd);
			if (cur) {
				Z_LVAL_P(cur) |= want;
			} else {
				zval z;
				ZVAL_LONG(&z, want);
				zend_hash_index_add_new(events_by_fd, (zend_ulong) fd, &z);
			}
			cnt++;
		}
	} ZEND_HASH_FOREACH_END();
	return cnt;
}

static int phasync_filter(zval *array, HashTable *revents_by_fd, short mask)
{
	zval *elem, *dest;
	HashTable *ht;
	zend_ulong num_ind;
	zend_string *key;
	int ret = 0;

	if (!array || Z_TYPE_P(array) != IS_ARRAY) {
		return 0;
	}
	ht = zend_new_array(zend_hash_num_elements(Z_ARRVAL_P(array)));

	ZEND_HASH_FOREACH_KEY_VAL(Z_ARRVAL_P(array), num_ind, key, elem) {
		php_socket_t fd = phasync_elem_fd(elem);
		if (fd != -1) {
			zval *rev = zend_hash_index_find(revents_by_fd, (zend_ulong) fd);
			if (rev && (Z_LVAL_P(rev) & mask)) {
				if (!key) {
					dest = zend_hash_index_update(ht, num_ind, elem);
				} else {
					dest = zend_hash_update(ht, key, elem);
				}
				zval_add_ref(dest);
				ret++;
			}
		}
	} ZEND_HASH_FOREACH_END();

	zval_ptr_dtor(array);
	ZVAL_ARR(array, ht);
	return ret;
}

static int phasync_emulate_read(zval *array)
{
	zval *elem, *dest;
	HashTable *ht;
	php_stream *stream;
	zend_ulong num_ind;
	zend_string *key;
	int ret = 0;

	if (!array || Z_TYPE_P(array) != IS_ARRAY) {
		return 0;
	}
	ht = zend_new_array(zend_hash_num_elements(Z_ARRVAL_P(array)));

	ZEND_HASH_FOREACH_KEY_VAL(Z_ARRVAL_P(array), num_ind, key, elem) {
		ZVAL_DEREF(elem);
		if (Z_TYPE_P(elem) == IS_LONG) {
			continue;   /* raw fds have no PHP-side buffer */
		}
		php_stream_from_zval_no_verify(stream, elem);
		if (stream == NULL) {
			continue;
		}
		if ((stream->writepos - stream->readpos) > 0) {
			if (!key) {
				dest = zend_hash_index_update(ht, num_ind, elem);
			} else {
				dest = zend_hash_update(ht, key, elem);
			}
			zval_add_ref(dest);
			ret++;
		}
	} ZEND_HASH_FOREACH_END();

	if (ret > 0) {
		zval_ptr_dtor(array);
		ZVAL_ARR(array, ht);
	} else {
		zend_array_destroy(ht);
	}
	return ret;
}

ZEND_FUNCTION(phasync_stream_select)
{
	zval *r_array, *w_array, *e_array;
	zend_long sec, usec = 0;
	bool secnull, usecnull = 1;
	HashTable events_by_fd, revents_by_fd;
	struct pollfd *fds = NULL;
	int nfds, i, retval, sets = 0, timeout_ms;

	ZEND_PARSE_PARAMETERS_START(4, 5)
		Z_PARAM_ARRAY_EX2(r_array, 1, 1, 0)
		Z_PARAM_ARRAY_EX2(w_array, 1, 1, 0)
		Z_PARAM_ARRAY_EX2(e_array, 1, 1, 0)
		Z_PARAM_LONG_OR_NULL(sec, secnull)
		Z_PARAM_OPTIONAL
		Z_PARAM_LONG_OR_NULL(usec, usecnull)
	ZEND_PARSE_PARAMETERS_END();

	zend_hash_init(&events_by_fd, 8, NULL, NULL, 0);
	sets += phasync_collect(r_array, &events_by_fd, POLLIN);
	sets += phasync_collect(w_array, &events_by_fd, POLLOUT);
	sets += phasync_collect(e_array, &events_by_fd, POLLPRI);

	if (sets == 0) {
		zend_hash_destroy(&events_by_fd);
		if ((r_array && zend_hash_num_elements(Z_ARRVAL_P(r_array)))
		 || (w_array && zend_hash_num_elements(Z_ARRVAL_P(w_array)))
		 || (e_array && zend_hash_num_elements(Z_ARRVAL_P(e_array)))) {
			RETURN_FALSE;
		}
		zend_value_error("No stream arrays were passed");
		RETURN_THROWS();
	}

	if (secnull && !usecnull && usec != 0) {
		zend_hash_destroy(&events_by_fd);
		zend_argument_value_error(5, "must be null when argument #4 ($seconds) is null");
		RETURN_THROWS();
	}
	if (!secnull) {
		if (sec < 0) {
			zend_hash_destroy(&events_by_fd);
			zend_argument_value_error(4, "must be greater than or equal to 0");
			RETURN_THROWS();
		} else if (usec < 0) {
			zend_hash_destroy(&events_by_fd);
			zend_argument_value_error(5, "must be greater than or equal to 0");
			RETURN_THROWS();
		}
		timeout_ms = (int) (sec * 1000 + (usec / 1000));
	} else {
		timeout_ms = -1;
	}

	if (r_array) {
		retval = phasync_emulate_read(r_array);
		if (retval > 0) {
			if (w_array) { zval_ptr_dtor(w_array); ZVAL_EMPTY_ARRAY(w_array); }
			if (e_array) { zval_ptr_dtor(e_array); ZVAL_EMPTY_ARRAY(e_array); }
			zend_hash_destroy(&events_by_fd);
			RETURN_LONG(retval);
		}
	}

	nfds = zend_hash_num_elements(&events_by_fd);
	fds = ecalloc(nfds, sizeof(struct pollfd));
	i = 0;
	{
		zend_ulong fd;
		zval *ev;
		ZEND_HASH_FOREACH_NUM_KEY_VAL(&events_by_fd, fd, ev) {
			fds[i].fd = (int) fd;
			fds[i].events = (short) Z_LVAL_P(ev);
			i++;
		} ZEND_HASH_FOREACH_END();
	}

	do {
		retval = poll(fds, nfds, timeout_ms);
	} while (retval == -1 && errno == EINTR);

	if (retval == -1) {
		php_error_docref(NULL, E_WARNING, "Unable to select [%d]: %s", errno, strerror(errno));
		efree(fds);
		zend_hash_destroy(&events_by_fd);
		RETURN_FALSE;
	}

	zend_hash_init(&revents_by_fd, nfds ? nfds : 8, NULL, NULL, 0);
	for (i = 0; i < nfds; i++) {
		if (fds[i].revents) {
			zval z;
			ZVAL_LONG(&z, fds[i].revents);
			zend_hash_index_update(&revents_by_fd, (zend_ulong) fds[i].fd, &z);
		}
	}

	if (r_array) phasync_filter(r_array, &revents_by_fd, POLLIN | POLLHUP | POLLERR);
	if (w_array) phasync_filter(w_array, &revents_by_fd, POLLOUT | POLLERR);
	if (e_array) phasync_filter(e_array, &revents_by_fd, POLLPRI);

	efree(fds);
	zend_hash_destroy(&revents_by_fd);
	zend_hash_destroy(&events_by_fd);
	RETURN_LONG(retval);
}

/* ---- module lifecycle ---------------------------------------------------- */

static PHP_GINIT_FUNCTION(phasync)
{
#if defined(COMPILE_DL_PHASYNC) && defined(ZTS)
	ZEND_TSRMLS_CACHE_UPDATE();
#endif
	memset(phasync_globals, 0, sizeof(*phasync_globals));
	ZVAL_UNDEF(&phasync_globals->read_handler);
	ZVAL_UNDEF(&phasync_globals->write_handler);
	/* Persistent + globals-lifetime: wrapped streams may be freed during request
	 * shutdown, after RSHUTDOWN, and their close op consults this table. */
	zend_hash_init(&phasync_globals->hooked, 8, NULL, NULL, 1);
}

static PHP_GSHUTDOWN_FUNCTION(phasync)
{
	zend_hash_destroy(&phasync_globals->hooked);
}

static PHP_RINIT_FUNCTION(phasync)
{
	ZVAL_UNDEF(&PHASYNC_G(read_handler));
	ZVAL_UNDEF(&PHASYNC_G(write_handler));
	PHASYNC_G(hooks_enabled) = 0;
	PHASYNC_G(wrapped_ready) = 0;
	zend_hash_clean(&PHASYNC_G(hooked));
	return SUCCESS;
}

static PHP_RSHUTDOWN_FUNCTION(phasync)
{
	if (PHASYNC_G(hooks_enabled)) {
		if (PHASYNC_G(orig_tcp)) {
			php_stream_xport_register("tcp", PHASYNC_G(orig_tcp));
		}
		if (PHASYNC_G(orig_unix)) {
			php_stream_xport_register("unix", PHASYNC_G(orig_unix));
		}
		PHASYNC_G(hooks_enabled) = 0;
	}
	if (!Z_ISUNDEF(PHASYNC_G(read_handler)))  {
		zval_ptr_dtor(&PHASYNC_G(read_handler));
		ZVAL_UNDEF(&PHASYNC_G(read_handler));
	}
	if (!Z_ISUNDEF(PHASYNC_G(write_handler))) {
		zval_ptr_dtor(&PHASYNC_G(write_handler));
		ZVAL_UNDEF(&PHASYNC_G(write_handler));
	}
	/* Do NOT destroy PHASYNC_G(hooked) here — streams close later in shutdown. */
	return SUCCESS;
}

zend_module_entry phasync_module_entry = {
	STANDARD_MODULE_HEADER,
	"phasync",
	ext_functions,
	NULL,              /* MINIT */
	NULL,              /* MSHUTDOWN */
	PHP_RINIT(phasync),
	PHP_RSHUTDOWN(phasync),
	NULL,              /* MINFO */
	"0.2.0-dev",
	PHP_MODULE_GLOBALS(phasync),
	PHP_GINIT(phasync),
	PHP_GSHUTDOWN(phasync),
	NULL,
	STANDARD_MODULE_PROPERTIES_EX
};

#ifdef COMPILE_DL_PHASYNC
# ifdef ZTS
ZEND_TSRMLS_CACHE_DEFINE()
# endif
ZEND_GET_MODULE(phasync)
#endif
