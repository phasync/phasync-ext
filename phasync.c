/* phasync extension — growable stream_select (Tier 1) + async stream hooks (later).
 *
 * phasync\stream_select() mirrors the built-in stream_select() semantics but uses
 * poll(2) internally, so it is not bounded by FD_SETSIZE. It accepts ordinary
 * stream resources (including the select-only sentinels the hooks will hand out).
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

/* Collect the fds referenced by one stream array into an fd->events map.
 * Returns the number of valid descriptor-backed streams found. */
static int phasync_collect(zval *array, HashTable *events_by_fd, short want)
{
	zval *elem;
	php_stream *stream;
	int cnt = 0;

	if (!array || Z_TYPE_P(array) != IS_ARRAY) {
		return 0;
	}

	ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(array), elem) {
		php_socket_t this_fd;

		ZVAL_DEREF(elem);
		php_stream_from_zval_no_verify(stream, elem);
		if (stream == NULL) {
			continue;
		}
		if (php_stream_cast(stream, PHP_STREAM_AS_FD_FOR_SELECT | PHP_STREAM_CAST_INTERNAL,
				(void *) &this_fd, 1) == SUCCESS && this_fd != -1) {
			zval *cur = zend_hash_index_find(events_by_fd, (zend_ulong) this_fd);
			if (cur) {
				Z_LVAL_P(cur) |= want;
			} else {
				zval z;
				ZVAL_LONG(&z, want);
				zend_hash_index_add_new(events_by_fd, (zend_ulong) this_fd, &z);
			}
			cnt++;
		}
	} ZEND_HASH_FOREACH_END();

	return cnt;
}

/* Rebuild a stream array to only those streams whose fd got one of `mask` in revents. */
static int phasync_filter(zval *array, HashTable *revents_by_fd, short mask)
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
		php_socket_t this_fd;

		ZVAL_DEREF(elem);
		php_stream_from_zval_no_verify(stream, elem);
		if (stream == NULL) {
			continue;
		}
		if (php_stream_cast(stream, PHP_STREAM_AS_FD_FOR_SELECT | PHP_STREAM_CAST_INTERNAL,
				(void *) &this_fd, 1) == SUCCESS && this_fd != -1) {
			zval *rev = zend_hash_index_find(revents_by_fd, (zend_ulong) this_fd);
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

/* Buffered-data hack, matching ext/standard: a read stream with data already in
 * PHP's buffer must be reported ready even though poll() on its fd would not. */
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
	int nfds, i, retval, sets = 0;
	int timeout_ms;

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
			/* arrays had entries but none were descriptor-backed */
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
		timeout_ms = -1; /* block indefinitely */
	}

	/* buffered-data shortcut for the read set */
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
		php_error_docref(NULL, E_WARNING, "Unable to select [%d]: %s",
			errno, strerror(errno));
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

zend_module_entry phasync_module_entry = {
	STANDARD_MODULE_HEADER,
	"phasync",
	ext_functions,
	NULL, NULL, NULL, NULL, NULL,
	"0.1.0-dev",
	STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_PHASYNC
ZEND_GET_MODULE(phasync)
#endif
