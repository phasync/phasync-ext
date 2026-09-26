/* phasync extension
 *
 * phasync\ext\stream_select() — growable, poll(2)-based, no FD_SETSIZE limit.
 *         Accepts stream resources and plain integer file descriptors.
 *
 * phasync\ext\manage($code, $read, $write, $sleep, $timeoutException) — run $code
 *         with transparent async I/O active for its dynamic extent. The tcp/unix/
 *         ssl transports are re-registered and proc_open()/stream_socket_pair()/
 *         sleep()/usleep()/time_nanosleep()/time_sleep_until()/gethostbyname()/
 *         fopen() overridden at request start, and every descriptor-backed stream
 *         is wrapped as it is created (plus STDIN/STDOUT/STDERR and any fds
 *         inherited before load). The wrappers are inert outside a scope: a
 *         wrapped stream then behaves exactly like an unwrapped one. Inside a
 *         scope, a would-block on a stream left in blocking mode calls the scope's
 *         handler with the stream RESOURCE and the time the native op would still
 *         wait (null = forever); the handler returns when the stream is ready, or
 *         throws. An exception instanceof $timeoutException is caught and the op
 *         finishes the way native PHP does on a socket timeout; any other
 *         exception propagates out of the hooked function. The C side never
 *         touches the fiber API — the userland callbacks own all suspension; it
 *         works because PHP fibers are stackful. Regular-file and DNS blocking and
 *         the FIFO open() rendezvous run on a worker thread pool (not
 *         readiness-pollable / unsolvable single-threaded), waking the fiber via a
 *         self-pipe.
 */
#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_streams.h"
#include "php_network.h"
#include "zend_exceptions.h"
#include "phasync_arginfo.h"

#include <poll.h>
#include <errno.h>
#include <math.h>
#include <sys/epoll.h>
#ifdef HAVE_ARPA_NAMESER_H
# include <arpa/nameser.h>
#endif
#ifdef HAVE_RESOLV_H
# include <resolv.h>
#endif
#include "ext/standard/php_dns.h"
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <pthread.h>
#include <signal.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <limits.h>

#define PHP_PHASYNC_VERSION "0.4.0-alpha13"

typedef struct {
	bool want_block;    /* caller's intended blocking mode (default: blocking) */
	signed char applied; /* fd mode we last forced: -1 unknown, 0 blocking, 1 non-blocking */
} phasync_hook_entry;

/* Wrapped ops with the original embedded right after it, so the original is
 * recoverable from any stream carrying these ops (including accepted sockets,
 * which inherit the listener's ops pointer without going through our wrap path). */
typedef struct {
	php_stream_ops ops;             /* MUST be first member */
	const php_stream_ops *orig;
} phasync_wops;

/* stream->ops points at the .ops member (first) of a phasync_wops. */
#define PHASYNC_ORIG(stream) (((phasync_wops *) (stream)->ops)->orig)

typedef enum {
	PHASYNC_MODE_RAW  = 0,   /* raw read/write on the fd (sockets, pipes)     */
	PHASYNC_MODE_TLS  = 1,   /* delegate to the original op (SSL_read/write)  */
	PHASYNC_MODE_POOL = 2    /* offload to the thread pool (regular files)    */
} phasync_mode;

/* One active phasync\manage() scope. Frames live on the C stack of the manage()
 * call and link to the enclosing scope, so nesting is plain LIFO and unwinds
 * automatically. The three handlers are owned copies of the closures. */
typedef struct phasync_scope {
	zval read;
	zval write;
	zval sleep;
	zend_class_entry *timeout_ce;   /* handler exceptions instanceof this = a timeout */
	struct phasync_scope *prev;
} phasync_scope;

ZEND_BEGIN_MODULE_GLOBALS(phasync)
	phasync_scope *scope_top;     /* innermost active manage() scope, or NULL */
	php_stream_transport_factory orig_tcp;
	php_stream_transport_factory orig_unix;
	php_stream_transport_factory orig_ssl;
	void (*orig_proc_open)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_sleep)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_usleep)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_time_nanosleep)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_time_sleep_until)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_gethostbyname)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_gethostbynamel)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_gethostbyaddr)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_dns_check_record)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_dns_get_record)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_dns_get_mx)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_fopen)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_stream_socket_pair)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_stream_select)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_socket_select)(INTERNAL_FUNCTION_PARAMETERS);
	int select_depth;             /* >0 while our override probes the original select */
	zend_long thread_pool_size;   /* INI: phasync.thread_pool_size */
	bool hooks_installed;         /* transports + fn overrides physically in place */
	HashTable hooked;             /* (uintptr_t)stream    -> phasync_hook_entry* */
	HashTable wrapped_ops_cache;  /* (uintptr_t)orig_ops  -> php_stream_ops*     */
ZEND_END_MODULE_GLOBALS(phasync)

ZEND_DECLARE_MODULE_GLOBALS(phasync)

#ifdef ZTS
# define PHASYNC_G(v) ZEND_MODULE_GLOBALS_ACCESSOR(phasync, v)
#else
# define PHASYNC_G(v) (phasync_globals.v)
#endif

/* Active handlers = those of the innermost manage() scope, or NULL when no
 * scope is active (then hooked I/O falls through to the original behaviour). */
static zend_always_inline zval *phasync_read_handler(void)
{
	return PHASYNC_G(scope_top) ? &PHASYNC_G(scope_top)->read : NULL;
}
static zend_always_inline zval *phasync_write_handler(void)
{
	return PHASYNC_G(scope_top) ? &PHASYNC_G(scope_top)->write : NULL;
}
static zend_always_inline zval *phasync_sleep_handler(void)
{
	return PHASYNC_G(scope_top) ? &PHASYNC_G(scope_top)->sleep : NULL;
}

/* ---- fd + wait helpers --------------------------------------------------- */

static php_socket_t phasync_stream_fd(php_stream *stream)
{
	php_socket_t fd = -1;
	php_stream_cast(stream, PHP_STREAM_AS_FD_FOR_SELECT | PHP_STREAM_CAST_INTERNAL,
		(void *) &fd, 0);
	return fd;
}

/* Tri-state result of a read/write wait handler. */
#define PHASYNC_WAIT_READY    0    /* handler said ready  -> retry the op          */
#define PHASYNC_WAIT_TIMEOUT  1    /* handler said timed out -> finish like native */
#define PHASYNC_WAIT_ERROR    (-1) /* handler threw -> exception pending, propagate */

/* Call the sleep handler as handler(int $microseconds). Returns -1 if it threw. */
static int phasync_call_sleep(zval *handler, zend_long usec)
{
	zval arg, retval;
	int rc = 0;

	ZVAL_LONG(&arg, usec);
	ZVAL_UNDEF(&retval);
	if (call_user_function(NULL, NULL, handler, &retval, 1, &arg) == FAILURE || EG(exception)) {
		rc = -1;
	}
	zval_ptr_dtor(&retval);
	return rc;
}

/* Call a read/write wait handler as handler(resource $stream, ?float $timeout).
 * $timeout is how many seconds the native op would still wait (null = forever).
 * The handler's return value is ignored: returning means the stream is ready
 * (PHASYNC_WAIT_READY, retry the op). If it throws an instance of the scope's
 * timeout class, the exception is cleared and the wait counts as timed out
 * (PHASYNC_WAIT_TIMEOUT — finish the op the native timeout way). Any other
 * exception is left pending (PHASYNC_WAIT_ERROR) so it propagates out of the
 * hooked function unchanged. */
static int phasync_call_wait(zval *handler, zval *arg, double timeout)
{
	zval call_args[2], retval;
	int rc = PHASYNC_WAIT_READY;

	ZVAL_COPY(&call_args[0], arg);        /* +1 ref for the duration of the call */
	if (isinf(timeout)) {
		ZVAL_NULL(&call_args[1]);
	} else {
		ZVAL_DOUBLE(&call_args[1], timeout);
	}
	ZVAL_UNDEF(&retval);
	if (call_user_function(NULL, NULL, handler, &retval, 2, call_args) == FAILURE) {
		rc = PHASYNC_WAIT_ERROR;
	} else if (EG(exception)) {
		zend_class_entry *tce = PHASYNC_G(scope_top) ? PHASYNC_G(scope_top)->timeout_ce : NULL;
		if (tce && instanceof_function(EG(exception)->ce, tce)) {
			zend_clear_exception();
			rc = PHASYNC_WAIT_TIMEOUT;
		} else {
			rc = PHASYNC_WAIT_ERROR;
		}
	}
	zval_ptr_dtor(&call_args[0]);
	zval_ptr_dtor(&retval);
	return rc;
}

/* If the stream is a socket, return its netstream data — which holds the timeout
 * and the timed-out flag — otherwise NULL. xp_socket's udp/unix/udg streams share
 * php_sockop_read as their read op. But ext/openssl registers itself for tcp://
 * too (so crypto can be enabled on a plain TCP stream later), so TCP and ssl/tls
 * streams use openssl's static ops, recognisable only by their label; its stream
 * data starts with a php_netstream_data_t (xp_ssl.c), so the cast holds. */
static php_netstream_data_t *phasync_sock_data(php_stream *stream)
{
	const php_stream_ops *orig = stream ? PHASYNC_ORIG(stream) : NULL;
	if (orig && stream->abstract
	 && (orig->read == php_stream_socket_ops.read
	  || (orig->label && strcmp(orig->label, "tcp_socket/ssl") == 0))) {
		return (php_netstream_data_t *) stream->abstract;
	}
	return NULL;
}

/* The timeout (seconds) native PHP would apply to a would-block wait here: the
 * socket's own timeout (stream_set_timeout()/default_socket_timeout; tv_sec == -1
 * means none), or INF for non-sockets (pipes/FIFOs/files block forever). */
static double phasync_wait_timeout(php_stream *stream)
{
	php_netstream_data_t *sock = phasync_sock_data(stream);
	if (sock == NULL || sock->timeout.tv_sec == -1) {
		return INFINITY;
	}
	return (double) sock->timeout.tv_sec + (double) sock->timeout.tv_usec / 1000000.0;
}

/* Flag the stream as timed out exactly as the native socket op does, so
 * stream_get_meta_data()['timed_out'] === true. No-op for non-sockets. */
static void phasync_mark_timed_out(php_stream *stream)
{
	php_netstream_data_t *sock = phasync_sock_data(stream);
	if (sock) {
		sock->timeout_event = true;
	}
}

/* Since 8.3, native php_sockop_read does not wait at all (MSG_DONTWAIT, returns 0)
 * when the current read call has already delivered buffered data, or when the
 * socket's timeout is exactly zero. Mirror that for sockets; pipes/files block
 * natively. 8.2 has no such rule: a blocking read always waits, and a zero timeout
 * times out at once — which the normal wait path already reproduces. */
static bool phasync_read_dont_wait(php_stream *stream)
{
#if PHP_VERSION_ID >= 80300
	php_netstream_data_t *sock = phasync_sock_data(stream);
	return sock && (stream->has_buffered_data
		|| (sock->timeout.tv_sec == 0 && sock->timeout.tv_usec == 0));
#else
	(void) stream;
	return false;
#endif
}

/* Native php_sockop_write reports a timed-out send with a notice; mirror it. */
static void phasync_write_timeout_notice(php_stream *stream, size_t count, int err)
{
	char *estr;

	if (!phasync_sock_data(stream) || (stream->flags & PHP_STREAM_FLAG_SUPPRESS_ERRORS)) {
		return;
	}
	estr = php_socket_strerror(err, NULL, 0);
#ifdef php_stream_warn
	php_stream_warn(stream, NetworkSendFailed,
		"Send of %zu bytes failed with errno=%d %s", count, err, estr);
#else
	php_error_docref(NULL, E_NOTICE,
		"Send of %zu bytes failed with errno=%d %s", count, err, estr);
#endif
	efree(estr);
}

static double phasync_now(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (double) ts.tv_sec + (double) ts.tv_nsec / 1e9;
}

/* Wait for the fd via the handler, handing over the stream RESOURCE (so it can go
 * straight into phasync::readable()/writable() or a native stream_select()) and
 * the native timeout. See phasync_call_wait for the tri-state return.
 *
 * For a real stream (socket/pipe/TLS) we borrow the stream's own resource. For a
 * bare fd with no stream — the thread pool's self-pipe — we wrap a dup() of the fd
 * in a transient php_stream so the loop still gets a selectable resource, then
 * close it afterwards (the dup keeps the pool's own fd untouched for reuse). */
static int phasync_wait_fd(zval *handler, php_stream *stream, php_socket_t fd, double timeout)
{
	zval arg;
	int rc;

	if (stream != NULL && stream->res != NULL) {
		ZVAL_RES(&arg, stream->res);   /* borrow (no ownership transfer) */
		return phasync_call_wait(handler, &arg, timeout);
	}

	if (fd == -1) {
		return PHASYNC_WAIT_ERROR;
	}
	{
		int dupfd = dup(fd);
		php_stream *tmp;
		if (dupfd < 0) {
			return PHASYNC_WAIT_ERROR;
		}
		tmp = php_stream_fopen_from_fd(dupfd, "r", NULL);
		if (tmp == NULL) {
			close(dupfd);
			return PHASYNC_WAIT_ERROR;
		}
		php_stream_to_zval(tmp, &arg);   /* register as a resource; arg owns 1 ref */
		rc = phasync_call_wait(handler, &arg, timeout);
		zval_ptr_dtor(&arg);             /* drop our ref -> closes tmp (and dupfd) */
		return rc;
	}
}

#define PHASYNC_COOP_ERROR    0   /* stop: exception pending          */
#define PHASYNC_COOP_RETRY    1   /* ready: retry the op              */
#define PHASYNC_COOP_TIMEOUT  2   /* stop: native timeout path taken  */

/* One cooperative wait inside a read/write op. *deadline carries the wait's
 * deadline across retries within one op call (one fill), so a spurious wake-up
 * is handed only the REMAINING time; callers start each op call with
 * *deadline < 0, giving every fill a fresh full timeout, as
 * php_sock_stream_wait_for_data does. On timeout the stream's timed-out flag is
 * set, exactly as the native socket op sets it. */
static int phasync_cooperate(zval *handler, php_stream *stream, php_socket_t fd, double *deadline)
{
	double timeout = phasync_wait_timeout(stream), wait = INFINITY;
	int w;

	if (!isinf(timeout)) {
		double now = phasync_now();
		if (*deadline < 0) {
			*deadline = now + timeout;
			wait = timeout;                /* first wait of this fill: the exact native timeout */
		} else {
			wait = *deadline - now;        /* a retry: only the remaining time */
		}
		if (wait <= 0) {                   /* time used up (or a zero timeout) */
			phasync_mark_timed_out(stream);
			return PHASYNC_COOP_TIMEOUT;
		}
	}
	w = phasync_wait_fd(handler, stream, fd, wait);
	if (w == PHASYNC_WAIT_READY) {
		return PHASYNC_COOP_RETRY;
	}
	if (w == PHASYNC_WAIT_TIMEOUT) {
		phasync_mark_timed_out(stream);
		return PHASYNC_COOP_TIMEOUT;
	}
	return PHASYNC_COOP_ERROR;
}

/* ---- thread pool: run blocking syscalls off the main thread --------------
 *
 * Workers only ever touch raw fds + plain buffers + the task struct — never the
 * Zend engine — so this is safe under a non-ZTS build. Completion is delivered
 * via a per-op self-pipe: the worker writes one byte to the pipe, which makes
 * the read end readable; the fiber was parked on that fd via the registered read
 * handler, so the scheduler resumes it. No lost-wakeup race: the byte is latched
 * in the pipe whether the worker finishes before or after the fiber parks.
 *
 * Fork safety: pthread_atfork() resets the pool in the child, because fork()
 * does not clone the worker threads and may copy the mutex in a locked state.
 * fork()+exec() (shell_exec/exec/proc_open) is unaffected — exec wipes the child
 * before it can touch the pool; only pcntl_fork() without exec needs the reset. */

/* Bounded worker pool for bounded-duration blocking syscalls (file read/write,
 * DNS). Size is INI-tunable (phasync.thread_pool_size); the cost of an idle
 * worker is a virtual stack reservation, so a larger pool is cheap — we also
 * cap each worker stack low (workers only call read/write/open/getaddrinfo).
 * FIFO open() is NOT run here: its rendezvous can block indefinitely and a
 * bounded pool would deadlock (all workers parked on read-opens while the
 * matching write-opens sit in the queue), so those get a dedicated thread. */
#define PHASYNC_POOL_MAX        128
#define PHASYNC_WORKER_STACK    (256 * 1024)

typedef enum {
	PHASYNC_OP_READ,
	PHASYNC_OP_WRITE,
	PHASYNC_OP_GETHOSTBYNAME,
	PHASYNC_OP_GETADDRINFO,
	PHASYNC_OP_NAMEINFO,
	PHASYNC_OP_DNSQUERY,
	PHASYNC_OP_OPEN            /* blocking open() (FIFO rendezvous), own thread   */
} phasync_op_type;

typedef struct phasync_task {
	phasync_op_type type;
	int          fd;          /* READ/WRITE; OPEN: resulting fd                 */
	char        *buf;         /* READ: dest (caller's buffer); WRITE: source   */
	size_t       count;
	ssize_t      result;
	int          err;
	const char  *host;        /* GETHOSTBYNAME input (plain C string)          */
	char         hostresult[NI_MAXHOST]; /* NAMEINFO result                       */
	int          hostok;
	struct in_addr addrs[64]; /* GETHOSTBYNAME: h_addr_list (IPv4)             */
	int          naddrs;
	struct sockaddr_storage sa; /* NAMEINFO input                              */
	socklen_t    salen;
	int          qtype;       /* DNSQUERY: record type; buf/count = answer buffer */
	struct addrinfo  hints;   /* GETADDRINFO input                             */
	struct addrinfo *ai;      /* GETADDRINFO result (caller freeaddrinfo()s)   */
	char         path[PATH_MAX]; /* OPEN: path (own copy, worker-stable)        */
	int          oflags;      /* OPEN: open(2) flags                           */
	int          omode;       /* OPEN: open(2) mode                            */
	int          write_fd;    /* self-pipe write end */
	struct phasync_task *next;
} phasync_task;

static pthread_mutex_t phasync_pool_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  phasync_pool_cond  = PTHREAD_COND_INITIALIZER;
static phasync_task   *phasync_q_head, *phasync_q_tail;
static pthread_t       phasync_workers[PHASYNC_POOL_MAX];
static int             phasync_pool_n;        /* live worker count             */
static int             phasync_pool_started; /* 0 = not yet, 1 = running */
static int             phasync_pool_stop;

/* Run one task's blocking syscall (shared by pool workers, the FIFO-open
 * dedicated thread, and the inline fallback). Touches only fds/buffers. */
#ifdef HAVE_FULL_DNS_FUNCS
/* ---- DNS: dns_get_record() / dns_get_mx() / dns_check_record() ------------
 *
 * Portions of this section are derived from PHP's ext/standard/dns.c:
 *   Copyright (c) The PHP Group and Contributors. SPDX-License-Identifier:
 *   BSD-3-Clause. See LICENSE.php-src for the full license text.
 * The parser and the functions' logic are kept as in PHP (identical from 8.2 to
 * master); only the resolver query itself runs on the worker pool. */

#ifndef DNS_T_A
#define DNS_T_A		1
#endif
#ifndef DNS_T_NS
#define DNS_T_NS	2
#endif
#ifndef DNS_T_CNAME
#define DNS_T_CNAME	5
#endif
#ifndef DNS_T_SOA
#define DNS_T_SOA	6
#endif
#ifndef DNS_T_PTR
#define DNS_T_PTR	12
#endif
#ifndef DNS_T_HINFO
#define DNS_T_HINFO	13
#endif
#ifndef DNS_T_MX
#define DNS_T_MX	15
#endif
#ifndef DNS_T_TXT
#define DNS_T_TXT	16
#endif
#ifndef DNS_T_AAAA
#define DNS_T_AAAA	28
#endif
#ifndef DNS_T_SRV
#define DNS_T_SRV	33
#endif
#ifndef DNS_T_NAPTR
#define DNS_T_NAPTR	35
#endif
#ifndef DNS_T_A6
#define DNS_T_A6	38
#endif
#ifndef DNS_T_CAA
#define DNS_T_CAA	257
#endif
#ifndef DNS_T_ANY
#define DNS_T_ANY	255
#endif
#ifndef HFIXEDSZ
#define HFIXEDSZ	12
#endif
#ifndef QFIXEDSZ
#define QFIXEDSZ	4
#endif
#define PHASYNC_DNS_MAXHOSTNAMELEN 1024

typedef union {
	HEADER qb1;
	uint8_t qb2[65536];
} phasync_querybuf;

/* php_dns_free_handle() (php_dns.h) calls this dns.c helper under res_nsearch. */
#if defined(__GLIBC__)
#define php_dns_free_res(__res__) phasync_dns_free_res(__res__)
static void phasync_dns_free_res(struct __res_state *res)
{
	int ns;
	for (ns = 0; ns < MAXNS; ns++) {
		if (res->_u._ext.nsaddrs[ns] != NULL) {
			free(res->_u._ext.nsaddrs[ns]);
			res->_u._ext.nsaddrs[ns] = NULL;
		}
	}
}
#else
#define php_dns_free_res(__res__)
#endif

/* Runs on a worker thread: pure libc, no engine. */
static void phasync_dns_query_worker(phasync_task *t)
{
#if defined(HAVE_RES_NSEARCH)
	struct __res_state state;
	struct __res_state *handle = &state;
	memset(&state, 0, sizeof(state));
	if (res_ninit(handle)) {
		t->hostok = 0;
		t->result = -1;
		return;
	}
#else
	void *handle = NULL;
	res_init();
#endif
	t->hostok = 1;
	t->result = php_dns_search(handle, t->host, C_IN, t->qtype, (u_char *) t->buf, (int) t->count);
	t->err = php_dns_errno(handle);
	php_dns_free_handle(handle);
	(void) handle;
}

static void phasync_pool_run(phasync_task *t);

/* The query on the pool (inline if there's no fiber to yield from). */
static int phasync_dns_search(const char *host, int type, phasync_querybuf *answer, int *dns_errno, int *init_ok)
{
	phasync_task t;
	memset(&t, 0, sizeof(t));
	t.type  = PHASYNC_OP_DNSQUERY;
	t.host  = host;
	t.qtype = type;
	t.buf   = (char *) answer->qb2;
	t.count = sizeof(*answer);
	phasync_pool_run(&t);
	*dns_errno = t.err;
	*init_ok = t.hostok;
	return (int) t.result;
}

#define CHECKCP(n) do { \
	if (cp + n > end) { \
		return NULL; \
	} \
} while (0)

static uint8_t *phasync_dns_parserr(uint8_t *cp, uint8_t *end, phasync_querybuf *answer, int type_to_fetch, int store, bool raw, zval *subarray)
{
	u_short type, class, dlen;
	u_long ttl;
	long n, i;
	u_short s;
	uint8_t *tp, *p;
	char name[PHASYNC_DNS_MAXHOSTNAMELEN] = {0};
	int have_v6_break = 0, in_v6_break = 0;

	ZVAL_UNDEF(subarray);

	n = dn_expand(answer->qb2, end, cp, name, sizeof(name) - 2);
	if (n < 0) {
		return NULL;
	}
	cp += n;

	CHECKCP(10);
	GETSHORT(type, cp);
	GETSHORT(class, cp);
	GETLONG(ttl, cp);
	GETSHORT(dlen, cp);
	CHECKCP(dlen);
	if (dlen == 0) {
		return NULL;
	}
	if (type_to_fetch != DNS_T_ANY && type != type_to_fetch) {
		cp += dlen;
		return cp;
	}
	if (!store) {
		cp += dlen;
		return cp;
	}

	array_init(subarray);
	add_assoc_string(subarray, "host", name);
	add_assoc_string(subarray, "class", "IN");
	add_assoc_long(subarray, "ttl", ttl);
	(void) class;

	if (raw) {
		add_assoc_long(subarray, "type", type);
		add_assoc_stringl(subarray, "data", (char*) cp, (uint32_t) dlen);
		cp += dlen;
		return cp;
	}

	switch (type) {
		case DNS_T_A:
			CHECKCP(4);
			add_assoc_string(subarray, "type", "A");
			snprintf(name, sizeof(name), "%d.%d.%d.%d", cp[0], cp[1], cp[2], cp[3]);
			add_assoc_string(subarray, "ip", name);
			cp += dlen;
			break;
		case DNS_T_MX:
			CHECKCP(2);
			add_assoc_string(subarray, "type", "MX");
			GETSHORT(n, cp);
			add_assoc_long(subarray, "pri", n);
			ZEND_FALLTHROUGH;
		case DNS_T_CNAME:
			if (type == DNS_T_CNAME) {
				add_assoc_string(subarray, "type", "CNAME");
			}
			ZEND_FALLTHROUGH;
		case DNS_T_NS:
			if (type == DNS_T_NS) {
				add_assoc_string(subarray, "type", "NS");
			}
			ZEND_FALLTHROUGH;
		case DNS_T_PTR:
			if (type == DNS_T_PTR) {
				add_assoc_string(subarray, "type", "PTR");
			}
			n = dn_expand(answer->qb2, end, cp, name, (sizeof name) - 2);
			if (n < 0) {
				return NULL;
			}
			cp += n;
			add_assoc_string(subarray, "target", name);
			break;
		case DNS_T_HINFO:
			add_assoc_string(subarray, "type", "HINFO");
			CHECKCP(1);
			n = *cp & 0xFF;
			cp++;
			CHECKCP(n);
			add_assoc_stringl(subarray, "cpu", (char*)cp, n);
			cp += n;
			CHECKCP(1);
			n = *cp & 0xFF;
			cp++;
			CHECKCP(n);
			add_assoc_stringl(subarray, "os", (char*)cp, n);
			cp += n;
			break;
		case DNS_T_CAA:
			add_assoc_string(subarray, "type", "CAA");
			CHECKCP(1);
			n = *cp & 0xFF;
			add_assoc_long(subarray, "flags", n);
			cp++;
			CHECKCP(1);
			n = *cp & 0xFF;
			cp++;
			CHECKCP(n);
			add_assoc_stringl(subarray, "tag", (char*)cp, n);
			cp += n;
			if ( (size_t) dlen < ((size_t)n) + 2 ) {
				return NULL;
			}
			n = dlen - n - 2;
			CHECKCP(n);
			add_assoc_stringl(subarray, "value", (char*)cp, n);
			cp += n;
			break;
		case DNS_T_TXT:
			{
				int l1 = 0, l2 = 0;
				zval entries;
				zend_string *tps;

				add_assoc_string(subarray, "type", "TXT");
				tps = zend_string_alloc(dlen, 0);
				array_init(&entries);
				while (l1 < dlen) {
					n = cp[l1];
					if ((l1 + n) >= dlen) {
						n = dlen - (l1 + 1);
					}
					if (n) {
						memcpy(ZSTR_VAL(tps) + l2 , cp + l1 + 1, n);
						add_next_index_stringl(&entries, (char *) cp + l1 + 1, n);
					}
					l1 = l1 + n + 1;
					l2 = l2 + n;
				}
				ZSTR_VAL(tps)[l2] = '\0';
				ZSTR_LEN(tps) = l2;
				cp += dlen;
				add_assoc_str(subarray, "txt", tps);
				add_assoc_zval(subarray, "entries", &entries);
			}
			break;
		case DNS_T_SOA:
			add_assoc_string(subarray, "type", "SOA");
			n = dn_expand(answer->qb2, end, cp, name, (sizeof name) -2);
			if (n < 0) {
				return NULL;
			}
			cp += n;
			add_assoc_string(subarray, "mname", name);
			n = dn_expand(answer->qb2, end, cp, name, (sizeof name) -2);
			if (n < 0) {
				return NULL;
			}
			cp += n;
			add_assoc_string(subarray, "rname", name);
			CHECKCP(5*4);
			GETLONG(n, cp);
			add_assoc_long(subarray, "serial", n);
			GETLONG(n, cp);
			add_assoc_long(subarray, "refresh", n);
			GETLONG(n, cp);
			add_assoc_long(subarray, "retry", n);
			GETLONG(n, cp);
			add_assoc_long(subarray, "expire", n);
			GETLONG(n, cp);
			add_assoc_long(subarray, "minimum-ttl", n);
			break;
		case DNS_T_AAAA:
			tp = (uint8_t*)name;
			CHECKCP(8*2);
			for(i=0; i < 8; i++) {
				GETSHORT(s, cp);
				if (s != 0) {
					if (tp > (uint8_t *)name) {
						in_v6_break = 0;
						tp[0] = ':';
						tp++;
					}
					tp += snprintf((char*)tp, sizeof(name) - (tp - (uint8_t *) name), "%x", s);
				} else {
					if (!have_v6_break) {
						have_v6_break = 1;
						in_v6_break = 1;
						tp[0] = ':';
						tp++;
					} else if (!in_v6_break) {
						tp[0] = ':';
						tp++;
						tp[0] = '0';
						tp++;
					}
				}
			}
			if (have_v6_break && in_v6_break) {
				tp[0] = ':';
				tp++;
			}
			tp[0] = '\0';
			add_assoc_string(subarray, "type", "AAAA");
			add_assoc_string(subarray, "ipv6", name);
			break;
		case DNS_T_A6:
			p = cp;
			add_assoc_string(subarray, "type", "A6");
			CHECKCP(1);
			n = ((int)cp[0]) & 0xFF;
			cp++;
			add_assoc_long(subarray, "masklen", n);
			tp = (uint8_t*)name;
			if (n > 15) {
				have_v6_break = 1;
				in_v6_break = 1;
				tp[0] = ':';
				tp++;
			}
			if (n % 16 > 8) {
				if (cp[0] != 0) {
					if (tp > (uint8_t *)name) {
						in_v6_break = 0;
						tp[0] = ':';
						tp++;
					}
					snprintf((char*)tp, sizeof(name) - (tp - (uint8_t *) name), "%x", cp[0] & 0xFF);
				} else {
					if (!have_v6_break) {
						have_v6_break = 1;
						in_v6_break = 1;
						tp[0] = ':';
						tp++;
					} else if (!in_v6_break) {
						tp[0] = ':';
						tp++;
						tp[0] = '0';
						tp++;
					}
				}
				cp++;
			}
			for (i = (n + 8) / 16; i < 8; i++) {
				CHECKCP(2);
				GETSHORT(s, cp);
				if (s != 0) {
					if (tp > (uint8_t *)name) {
						in_v6_break = 0;
						tp[0] = ':';
						tp++;
					}
					tp += snprintf((char*)tp, sizeof(name) - (tp - (uint8_t *) name),"%x",s);
				} else {
					if (!have_v6_break) {
						have_v6_break = 1;
						in_v6_break = 1;
						tp[0] = ':';
						tp++;
					} else if (!in_v6_break) {
						tp[0] = ':';
						tp++;
						tp[0] = '0';
						tp++;
					}
				}
			}
			if (have_v6_break && in_v6_break) {
				tp[0] = ':';
				tp++;
			}
			tp[0] = '\0';
			add_assoc_string(subarray, "ipv6", name);
			if (cp < p + dlen) {
				n = dn_expand(answer->qb2, end, cp, name, (sizeof name) - 2);
				if (n < 0) {
					return NULL;
				}
				cp += n;
				add_assoc_string(subarray, "chain", name);
			}
			break;
		case DNS_T_SRV:
			CHECKCP(3*2);
			add_assoc_string(subarray, "type", "SRV");
			GETSHORT(n, cp);
			add_assoc_long(subarray, "pri", n);
			GETSHORT(n, cp);
			add_assoc_long(subarray, "weight", n);
			GETSHORT(n, cp);
			add_assoc_long(subarray, "port", n);
			n = dn_expand(answer->qb2, end, cp, name, (sizeof name) - 2);
			if (n < 0) {
				return NULL;
			}
			cp += n;
			add_assoc_string(subarray, "target", name);
			break;
		case DNS_T_NAPTR:
			CHECKCP(2*2);
			add_assoc_string(subarray, "type", "NAPTR");
			GETSHORT(n, cp);
			add_assoc_long(subarray, "order", n);
			GETSHORT(n, cp);
			add_assoc_long(subarray, "pref", n);
			CHECKCP(1);
			n = (cp[0] & 0xFF);
			cp++;
			CHECKCP(n);
			add_assoc_stringl(subarray, "flags", (char*)cp, n);
			cp += n;
			CHECKCP(1);
			n = (cp[0] & 0xFF);
			cp++;
			CHECKCP(n);
			add_assoc_stringl(subarray, "services", (char*)cp, n);
			cp += n;
			CHECKCP(1);
			n = (cp[0] & 0xFF);
			cp++;
			CHECKCP(n);
			add_assoc_stringl(subarray, "regex", (char*)cp, n);
			cp += n;
			n = dn_expand(answer->qb2, end, cp, name, (sizeof name) - 2);
			if (n < 0) {
				return NULL;
			}
			cp += n;
			add_assoc_string(subarray, "replacement", name);
			break;
		default:
			zval_ptr_dtor(subarray);
			ZVAL_UNDEF(subarray);
			cp += dlen;
			break;
	}
	return cp;
}
#undef CHECKCP
#endif /* HAVE_FULL_DNS_FUNCS */

static void phasync_task_exec(phasync_task *t)
{
	switch (t->type) {
		case PHASYNC_OP_READ:
			do { t->result = read(t->fd, t->buf, t->count); }
			while (t->result < 0 && errno == EINTR);
			t->err = errno;
			break;
		case PHASYNC_OP_WRITE:
			do { t->result = write(t->fd, t->buf, t->count); }
			while (t->result < 0 && errno == EINTR);
			t->err = errno;
			break;
		case PHASYNC_OP_OPEN:
			do { t->fd = open(t->path, t->oflags, t->omode); }
			while (t->fd < 0 && errno == EINTR);
			t->err = errno;
			break;
		case PHASYNC_OP_GETHOSTBYNAME: {
			/* php_network_gethostbyname() is gethostbyname_r() on Linux: use the
			 * same call so the addresses (and their order) are exactly native. */
			struct hostent he, *res = NULL;
			char stackbuf[8192], *buf = stackbuf;
			size_t buflen = sizeof(stackbuf);
			int herr = 0, rc;
			while ((rc = gethostbyname_r(t->host, &he, buf, buflen, &res, &herr)) == ERANGE
			       && buflen < (1 << 20)) {
				buflen *= 2;
				buf = (buf == stackbuf) ? malloc(buflen) : realloc(buf, buflen);
				if (!buf) break;
			}
			t->hostok = (rc == 0 && res != NULL);
			t->naddrs = 0;
			if (t->hostok && res->h_addrtype == AF_INET) {
				for (int i = 0; res->h_addr_list[i] && t->naddrs < (int) (sizeof(t->addrs) / sizeof(t->addrs[0])); i++) {
					memcpy(&t->addrs[t->naddrs++], res->h_addr_list[i], sizeof(struct in_addr));
				}
			}
			if (buf && buf != stackbuf) free(buf);
			break;
		}
#ifdef HAVE_FULL_DNS_FUNCS
		case PHASYNC_OP_DNSQUERY:
			/* Exactly the resolver calls native makes (php_dns.h picks them from how
			 * PHP was built): result in t->result, php_dns_errno() in t->err,
			 * t->hostok = 0 if the resolver state could not even be initialised. */
			phasync_dns_query_worker(t);
			break;
#endif
		case PHASYNC_OP_NAMEINFO:
			t->hostok = getnameinfo((struct sockaddr *) &t->sa, t->salen, t->hostresult,
				sizeof(t->hostresult), NULL, 0, NI_NAMEREQD) == 0;
			break;
		case PHASYNC_OP_GETADDRINFO: {
			t->ai = NULL;
			t->result = getaddrinfo(t->host, NULL, &t->hints, &t->ai);
			break;
		}
	}
}

/* Self-pipe freelist. Each pool op needs a pipe to wake its fiber; rather than
 * pipe()/close() per op (a real cost for many-small-reads like fgetcsv), we keep
 * a freelist that grows to the high-water mark of concurrent ops and is never
 * shrunk until shutdown. Touched only by the main (fiber) thread — workers write
 * to the fd but never touch the list — so no lock is needed. The fds are
 * FD_CLOEXEC so idle pipes never leak into shell_exec/proc_open children. */
typedef struct phasync_pipe {
	int rfd, wfd;
	struct phasync_pipe *next;
} phasync_pipe;

static phasync_pipe *phasync_pipe_free;

static phasync_pipe *phasync_pipe_get(void)
{
	phasync_pipe *p = phasync_pipe_free;
	int fds[2];

	if (p) {
		phasync_pipe_free = p->next;   /* reuse: it was drained before release */
		return p;
	}
	if (pipe(fds) != 0) {
		return NULL;
	}
	fcntl(fds[0], F_SETFD, FD_CLOEXEC);
	fcntl(fds[1], F_SETFD, FD_CLOEXEC);
	p = malloc(sizeof(*p));
	if (!p) {
		close(fds[0]);
		close(fds[1]);
		return NULL;
	}
	p->rfd = fds[0];
	p->wfd = fds[1];
	return p;
}

static void phasync_pipe_put(phasync_pipe *p)
{
	p->next = phasync_pipe_free;
	phasync_pipe_free = p;
}

/* Notify the parked fiber that a task is done (self-pipe: one byte). */
static void phasync_task_signal(phasync_task *t)
{
	char c = 1;
	ssize_t w;
	do { w = write(t->write_fd, &c, 1); } while (w < 0 && errno == EINTR);
	(void) w;
}

/* Block all signals on a helper thread, leaving them to the main thread. */
static void phasync_thread_block_signals(void)
{
	sigset_t set;
	sigfillset(&set);
	pthread_sigmask(SIG_BLOCK, &set, NULL);
}

static void *phasync_worker(void *arg)
{
	(void) arg;
	phasync_thread_block_signals();

	for (;;) {
		phasync_task *t;
		pthread_mutex_lock(&phasync_pool_mutex);
		while (phasync_q_head == NULL && !phasync_pool_stop) {
			pthread_cond_wait(&phasync_pool_cond, &phasync_pool_mutex);
		}
		if (phasync_pool_stop && phasync_q_head == NULL) {
			pthread_mutex_unlock(&phasync_pool_mutex);
			return NULL;
		}
		t = phasync_q_head;
		phasync_q_head = t->next;
		if (phasync_q_head == NULL) {
			phasync_q_tail = NULL;
		}
		pthread_mutex_unlock(&phasync_pool_mutex);

		phasync_task_exec(t);
		phasync_task_signal(t);   /* worker never touches the Zend engine */
	}
}

/* Dedicated one-shot thread for a single indefinitely-blocking task (FIFO
 * open). It runs the task, signals the self-pipe, and exits; the fiber joins
 * it after being resumed, so nothing leaks. */
static void *phasync_oneshot(void *arg)
{
	phasync_task *t = (phasync_task *) arg;
	phasync_thread_block_signals();

	/* Allow cancellation ONLY across the blocking syscall (open() is a POSIX
	 * cancellation point). If cancelled there, the thread unwinds inside open()
	 * before t->fd is ever assigned, so no fd leaks; the signal step below never
	 * runs. Everything outside is cancel-disabled so there is no window where a
	 * cancel could strand an already-opened fd. */
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	phasync_task_exec(t);
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

	phasync_task_signal(t);   /* worker never touches the Zend engine */
	return NULL;
}

static void phasync_pool_child_atfork(void)
{
	/* Runs in the forked child: workers are gone and the mutex may be locked.
	 * Reset everything so the child is clean (and will lazily rebuild a pool
	 * only if it actually uses one). */
	pthread_mutex_init(&phasync_pool_mutex, NULL);
	pthread_cond_init(&phasync_pool_cond, NULL);
	phasync_q_head = phasync_q_tail = NULL;
	phasync_pool_started = 0;
	phasync_pool_stop = 0;
	phasync_pool_n = 0;
	/* Close inherited self-pipe fds so they don't linger in the child; drop the
	 * freelist nodes (free() is not async-signal-safe, so we intentionally leak
	 * the small node structs — the child typically execs or exits shortly). */
	while (phasync_pipe_free) {
		phasync_pipe *p = phasync_pipe_free;
		phasync_pipe_free = p->next;
		close(p->rfd);
		close(p->wfd);
	}
	phasync_pipe_free = NULL;
}

/* Start the worker pool once. Returns non-zero if at least one worker exists. If
 * none could be created (thread/pid limits), the pool is left unstarted so a later
 * call retries, and the caller runs the task inline instead of queueing it where
 * nothing would ever pick it up. */
static int phasync_pool_ensure(void)
{
	int i, n;
	pthread_attr_t attr;
	if (phasync_pool_started) {
		return 1;
	}
	n = (int) PHASYNC_G(thread_pool_size);
	if (n < 1) n = 1;
	if (n > PHASYNC_POOL_MAX) n = PHASYNC_POOL_MAX;

	pthread_attr_init(&attr);
	pthread_attr_setstacksize(&attr, PHASYNC_WORKER_STACK);

	phasync_pool_stop = 0;
	phasync_pool_n = 0;
	for (i = 0; i < n; i++) {
		if (pthread_create(&phasync_workers[i], &attr, phasync_worker, NULL) != 0) {
			break;
		}
		phasync_pool_n++;
	}
	pthread_attr_destroy(&attr);
	if (phasync_pool_n == 0) {
		return 0;
	}
	phasync_pool_started = 1;
	pthread_atfork(NULL, NULL, phasync_pool_child_atfork);
	return 1;
}

static void phasync_pool_shutdown(void)
{
	int i;
	if (!phasync_pool_started) {
		return;
	}
	pthread_mutex_lock(&phasync_pool_mutex);
	phasync_pool_stop = 1;
	pthread_cond_broadcast(&phasync_pool_cond);
	pthread_mutex_unlock(&phasync_pool_mutex);
	for (i = 0; i < phasync_pool_n; i++) {
		pthread_join(phasync_workers[i], NULL);
	}
	phasync_pool_n = 0;
	phasync_pool_started = 0;

	/* Release the self-pipe freelist (MSHUTDOWN context: free() is fine here). */
	while (phasync_pipe_free) {
		phasync_pipe *p = phasync_pipe_free;
		phasync_pipe_free = p->next;
		close(p->rfd);
		close(p->wfd);
		free(p);
	}
}

static void phasync_pool_submit(phasync_task *t)
{
	pthread_mutex_lock(&phasync_pool_mutex);
	t->next = NULL;
	if (phasync_q_tail) {
		phasync_q_tail->next = t;
	} else {
		phasync_q_head = t;
	}
	phasync_q_tail = t;
	pthread_cond_signal(&phasync_pool_cond);
	pthread_mutex_unlock(&phasync_pool_mutex);
}

/* Submit a task, park the fiber on the self-pipe until the worker completes.
 * The task lives on the caller's (fiber) stack, which is preserved across the
 * suspend, so no heap allocation is needed for it. */
/* Block until the worker's completion byte arrives and consume it. Polls on
 * EAGAIN so it holds even if the read end was made non-blocking (it shares its
 * open file description with the dup() handed to the wait handler). */
static void phasync_pipe_drain(int rfd)
{
	char c;
	for (;;) {
		ssize_t r = read(rfd, &c, 1);
		if (r == 1 || r == 0) {
			return;
		}
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			struct pollfd pfd = { .fd = rfd, .events = POLLIN };
			poll(&pfd, 1, -1);
		} else if (errno != EINTR) {
			return;
		}
	}
}

static void phasync_pool_run(phasync_task *t)
{
	phasync_pipe *p;

	if (phasync_read_handler() == NULL || EG(active_fiber) == NULL
	 || (p = phasync_pipe_get()) == NULL) {
		/* No fiber to yield from (or no scheduler / pipe failed): run inline. */
		phasync_task_exec(t);
		return;
	}

	if (!phasync_pool_ensure()) {
		phasync_pipe_put(p);
		phasync_task_exec(t);            /* no worker could be started: run inline */
		return;
	}
	t->write_fd = p->wfd;
	phasync_pool_submit(t);

	/* Park until the worker makes the read end readable. The task (and a read's
	 * buffer) live in the caller's C frame and the worker now holds pointers into
	 * them, so a fatal error (bailout) raised inside the handler must not unwind
	 * past this frame before the worker is done: wait for its byte, then re-raise. */
	zend_try {
		phasync_wait_fd(phasync_read_handler(), NULL, p->rfd, INFINITY);
	} zend_catch {
		phasync_pipe_drain(p->rfd);
		phasync_pipe_put(p);
		zend_bailout();
	} zend_end_try();

	/* The worker finishes this bounded op shortly and writes its byte even if the
	 * fiber was resumed early by an exception, so draining here is safe and leaves
	 * the pipe empty for reuse. */
	phasync_pipe_drain(p->rfd);
	phasync_pipe_put(p);
}

/* Like phasync_pool_run, but on a dedicated thread rather than the bounded pool.
 * Used for FIFO open(), whose rendezvous can block indefinitely — a bounded
 * pool would deadlock when reader- and writer-opens outnumber the workers. */
static void phasync_pool_run_dedicated(phasync_task *t)
{
	int pipefd[2];
	pthread_t th;
	pthread_attr_t attr;
	int rc;

	if (phasync_read_handler() == NULL || EG(active_fiber) == NULL
	 || pipe(pipefd) != 0) {
		phasync_task_exec(t);   /* no fiber/scheduler: block inline, as fopen() would */
		return;
	}
	fcntl(pipefd[0], F_SETFD, FD_CLOEXEC);
	fcntl(pipefd[1], F_SETFD, FD_CLOEXEC);

	t->write_fd = pipefd[1];
	pthread_attr_init(&attr);
	pthread_attr_setstacksize(&attr, PHASYNC_WORKER_STACK);
	if (pthread_create(&th, &attr, phasync_oneshot, t) != 0) {
		pthread_attr_destroy(&attr);
		close(pipefd[0]);
		close(pipefd[1]);
		phasync_task_exec(t);
		return;
	}
	pthread_attr_destroy(&attr);

	/* A fatal error (bailout) raised inside the handler must not unwind past this
	 * frame while the thread still holds a pointer to the stack-resident task:
	 * cancel and reap it first, drop any fd it opened, then re-raise. */
	zend_try {
		rc = phasync_wait_fd(phasync_read_handler(), NULL, pipefd[0], INFINITY);
	} zend_catch {
		pthread_cancel(th);
		pthread_join(th, NULL);
		close(pipefd[0]);
		close(pipefd[1]);
		if (t->fd >= 0) {
			close(t->fd);
		}
		t->fd = -1;
		zend_bailout();
	} zend_end_try();

	/* Always reap the thread, however the fiber came back. If the worker already
	 * finished, it is past its (only) open() cancellation point with cancellation
	 * disabled, so the cancel is a no-op and join returns at once. If the fiber
	 * was resumed early (phasync timeout/cancel, or any other reason) the thread
	 * is still in open() — the cancel unblocks it there. Either way, no hang and
	 * no leaked thread; unconditional cancel avoids a blocking pipe read that
	 * would hang if the fiber were ever resumed without the pipe being ready. */
	pthread_cancel(th);
	pthread_join(th, NULL);
	close(pipefd[0]);
	close(pipefd[1]);

	/* The exception state (not the pipe) decides keep-vs-discard: on a pending
	 * exception, drop any fd the open managed to produce and let it propagate. */
	if (rc != 0) {
		if (t->fd >= 0) {
			close(t->fd);
		}
		t->fd = -1;
		return;
	}
	/* Normal return: the worker left t->fd as a valid fd, or -1 with t->err set
	 * (a real open() failure); the caller distinguishes the two. */
}

/* Pool-mode ops: offload the blocking read/write to a worker thread (for regular
 * files, which are not readiness-pollable). Falls back to the original op when
 * there's no scheduler or no fd. */
static ssize_t phasync_wrapped_read_pool(php_stream *stream, char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	phasync_task t;

	/* Outside a scope (or not in a fiber) there is nothing to yield to: be the
	 * native op exactly. */
	if (fd == -1 || phasync_read_handler() == NULL || EG(active_fiber) == NULL) {
		return orig->read(stream, buf, count);
	}
	memset(&t, 0, sizeof(t));
	t.type = PHASYNC_OP_READ;
	t.fd = fd;
	t.buf = buf;
	t.count = count;
	phasync_pool_run(&t);
	errno = t.err;
	if (t.result == 0) {
		stream->eof = 1;   /* a 0-byte read on a regular file is EOF (as plain
		                    * stdio read does); without this feof() never trips
		                    * and while (!feof($fp)) spins. */
	}
	return t.result;
}

static ssize_t phasync_wrapped_write_pool(php_stream *stream, const char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	phasync_task t;

	if (fd == -1 || phasync_read_handler() == NULL || EG(active_fiber) == NULL) {
		return orig->write(stream, buf, count);
	}
	memset(&t, 0, sizeof(t));
	t.type = PHASYNC_OP_WRITE;
	t.fd = fd;
	t.buf = (char *) buf;
	t.count = count;
	phasync_pool_run(&t);
	errno = t.err;
	return t.result;
}

/* ---- wrapped stream ops (shared across socket + pipe originals) ----------- */

static phasync_hook_entry *phasync_entry(php_stream *stream)
{
	return zend_hash_index_find_ptr(&PHASYNC_G(hooked), (zend_ulong) (uintptr_t) stream);
}

static phasync_hook_entry *phasync_entry_ensure(php_stream *stream)
{
	phasync_hook_entry *e = phasync_entry(stream);
	if (e == NULL) {
		e = pemalloc(sizeof(*e), 1);
		e->want_block  = true;    /* streams are blocking until told otherwise */
		e->applied     = -1;      /* fd mode not forced yet */
		zend_hash_index_add_ptr(&PHASYNC_G(hooked), (zend_ulong) (uintptr_t) stream, e);
	}
	return e;
}

/* Force the fd (and, for a delegate/TLS stream, the stream layer) into the given
 * blocking mode, but only when it differs from what we last applied — so the hot
 * path (repeated cooperative reads on an already-non-blocking fd) does no syscall.
 * We go through the ORIGINAL set_option, never php_stream_set_option, to avoid
 * re-entering our own BLOCKING interception. */
static void phasync_apply_mode(php_stream *stream, php_socket_t fd,
                               phasync_hook_entry *e, bool nonblock, bool tls)
{
	int flags;

	if (fd == -1 || e->applied == (signed char) (nonblock ? 1 : 0)) {
		return;
	}
	if (tls) {
		const php_stream_ops *orig = PHASYNC_ORIG(stream);
		if (orig->set_option) {
			orig->set_option(stream, PHP_STREAM_OPTION_BLOCKING, nonblock ? 0 : 1, NULL);
		}
	}
	flags = fcntl(fd, F_GETFL, 0);
	if (flags != -1) {
		if (nonblock) {
			flags |= O_NONBLOCK;
		} else {
			flags &= ~O_NONBLOCK;
		}
		fcntl(fd, F_SETFL, flags);
	}
	e->applied = nonblock ? 1 : 0;
}

/* The wrapped read/write ops emulate the caller's intended semantics:
 *   - outside a manage() scope (handler == NULL) -> exactly the native op;
 *   - an explicitly non-blocking stream          -> exactly the native op;
 *   - a blocking stream inside a scope           -> cooperate: drive the fd
 *     non-blocking and suspend the fiber on would-block instead of blocking.
 * So a wrapped stream is indistinguishable from an unwrapped one whenever no
 * scope is driving it. */
static ssize_t phasync_wrapped_read(php_stream *stream, char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	zval *handler = phasync_read_handler();
	phasync_hook_entry *e;

	if (fd == -1) {
		return orig->read(stream, buf, count);
	}
	e = phasync_entry_ensure(stream);
	if (!e->want_block || handler == NULL) {
		phasync_apply_mode(stream, fd, e, !e->want_block, false);
		return orig->read(stream, buf, count);
	}
	double deadline = -1;              /* per op call = per fill */
	phasync_apply_mode(stream, fd, e, true, false);
	for (;;) {
		ssize_t n = read(fd, buf, count);
		if (n > 0) {
			return n;
		}
		if (n == 0) {
			stream->eof = 1;
			return 0;
		}
		if (errno == EINTR) {
			continue;
		}
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			stream->eof = 1;
			return -1;
		}
		if (phasync_read_dont_wait(stream)) {
			return 0;                    /* native MSG_DONTWAIT short read */
		}
		if (phasync_cooperate(handler, stream, fd, &deadline) != PHASYNC_COOP_RETRY) {
			return -1;                   /* timed out (flag set) or exception pending */
		}
	}
}

static ssize_t phasync_wrapped_write(php_stream *stream, const char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	zval *handler = phasync_write_handler();
	phasync_hook_entry *e;

	if (fd == -1) {
		return orig->write(stream, buf, count);
	}
	e = phasync_entry_ensure(stream);
	if (!e->want_block || handler == NULL) {
		phasync_apply_mode(stream, fd, e, !e->want_block, false);
		return orig->write(stream, buf, count);
	}
	double deadline = -1;              /* per op call = per fill */
	phasync_apply_mode(stream, fd, e, true, false);
	for (;;) {
		ssize_t n = write(fd, buf, count);
		int err, c;
		if (n >= 0) {
			return n;
		}
		if (errno == EINTR) {
			continue;
		}
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			return -1;
		}
		err = errno;
		c = phasync_cooperate(handler, stream, fd, &deadline);
		if (c == PHASYNC_COOP_RETRY) {
			continue;
		}
		if (c == PHASYNC_COOP_TIMEOUT) {
			phasync_write_timeout_notice(stream, count, err);
		}
		return -1;
	}
}

/* Delegate-mode ops for streams whose bytes must go through the original op
 * (e.g. TLS: SSL_read/SSL_write). We can't raw read/write the fd, so inside a
 * scope we drive the stream non-blocking and let the original op report
 * would-block (0 without eof), then wait and retry. Read/write intent is
 * approximated (a read waits for readability); TLS renegotiation wanting the
 * opposite direction is a known limitation. */
static ssize_t phasync_wrapped_read_tls(php_stream *stream, char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	zval *handler = phasync_read_handler();
	phasync_hook_entry *e = phasync_entry_ensure(stream);

	if (!e->want_block || handler == NULL) {
		phasync_apply_mode(stream, fd, e, !e->want_block, true);
		return orig->read(stream, buf, count);
	}
	double deadline = -1;              /* per op call = per fill */
	phasync_apply_mode(stream, fd, e, true, true);
	for (;;) {
		ssize_t n = orig->read(stream, buf, count);
		if (n > 0) {
			return n;
		}
		if (n < 0 || stream->eof) {
			return n;                    /* error or real EOF */
		}
		fd = phasync_stream_fd(stream);  /* n == 0, not eof -> would block */
		if (fd == -1 || phasync_cooperate(handler, stream, fd, &deadline) != PHASYNC_COOP_RETRY) {
			return -1;
		}
	}
}

static ssize_t phasync_wrapped_write_tls(php_stream *stream, const char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	zval *handler = phasync_write_handler();
	phasync_hook_entry *e = phasync_entry_ensure(stream);

	if (!e->want_block || handler == NULL) {
		phasync_apply_mode(stream, fd, e, !e->want_block, true);
		return orig->write(stream, buf, count);
	}
	double deadline = -1;              /* per op call = per fill */
	phasync_apply_mode(stream, fd, e, true, true);
	for (;;) {
		ssize_t n = orig->write(stream, buf, count);
		if (n > 0) {
			return n;
		}
		if (n < 0) {
			return n;
		}
		fd = phasync_stream_fd(stream);  /* 0 -> would block */
		if (fd == -1 || phasync_cooperate(handler, stream, fd, &deadline) != PHASYNC_COOP_RETRY) {
			return -1;
		}
	}
}

/* Delegate a set_option call to the original ops. xport ops (bind/connect/listen/
 * accept/getname/…) all arrive through set_option, and the socket layer decides
 * unix-vs-inet by *pointer identity* (php_stream_is(stream,
 * &php_stream_unix_socket_ops), xp_socket.c). Our wrapped ops is a copy with a
 * different address, which would make a unix socket look like inet ("Failed to
 * parse address"), so restore the real ops pointer for the (synchronous) call. */
static int phasync_orig_set_option(php_stream *stream, int option, int value, void *ptrparam)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream), *saved = stream->ops;
	int r;

	if (!orig->set_option) {
		return PHP_STREAM_OPTION_RETURN_NOTIMPL;
	}
	stream->ops = (php_stream_ops *) orig;
	r = orig->set_option(stream, option, value, ptrparam);
	stream->ops = saved;
	return r;
}

/* stream_socket_accept() with a timeout, inside a scope and in a fiber: wait for
 * the listener to become readable via the read handler instead of blocking the
 * process, then accept with a zero timeout. On timeout, report exactly what the
 * socket layer reports (php_network_accept_incoming: ETIMEDOUT + its error text),
 * so the calling PHP function emits its own native warning. A zero timeout keeps
 * its explicit don't-wait meaning and goes straight to the original. */
static int phasync_accept_cooperative(php_stream *stream, php_stream_xport_param *xp)
{
	struct timeval *tv = xp->inputs.timeout, zero = { 0, 0 };
	double deadline = tv ? phasync_now() + (double) tv->tv_sec + (double) tv->tv_usec / 1000000.0 : 0;
	php_socket_t fd = phasync_stream_fd(stream);

	if (fd == -1 || (tv && tv->tv_sec == 0 && tv->tv_usec == 0)) {
		return phasync_orig_set_option(stream, PHP_STREAM_OPTION_XPORT_API, 0, xp);
	}
	for (;;) {
		double remaining = tv ? deadline - phasync_now() : INFINITY;
		int w = remaining > 0
			? phasync_wait_fd(phasync_read_handler(), stream, fd, remaining)
			: PHASYNC_WAIT_TIMEOUT;
		int r;

		if (w == PHASYNC_WAIT_TIMEOUT) {
			xp->outputs.client = NULL;
			xp->outputs.returncode = -1;
			xp->outputs.error_code = ETIMEDOUT;
			if (xp->want_errortext) {
				xp->outputs.error_text = php_socket_error_str(ETIMEDOUT);
			}
			return PHP_STREAM_OPTION_RETURN_OK;
		}
		if (w == PHASYNC_WAIT_ERROR) {
			xp->outputs.client = NULL;
			xp->outputs.returncode = -1;
			return PHP_STREAM_OPTION_RETURN_OK;   /* exception pending: propagates */
		}
		xp->inputs.timeout = &zero;
		r = phasync_orig_set_option(stream, PHP_STREAM_OPTION_XPORT_API, 0, xp);
		xp->inputs.timeout = tv;
		if (r != PHP_STREAM_OPTION_RETURN_OK || xp->outputs.client
		 || (xp->outputs.error_code != ETIMEDOUT && xp->outputs.error_code != EAGAIN
		     && xp->outputs.error_code != EWOULDBLOCK)) {
			return r;
		}
		/* Someone else took the connection between readiness and accept: wait again. */
		if (xp->outputs.error_text) {
			zend_string_release(xp->outputs.error_text);
			xp->outputs.error_text = NULL;
		}
	}
}

/* tcp:// connect (stream_socket_client/fsockopen/pfsockopen), inside a scope and
 * in a fiber. Native resolves the host (blocking getaddrinfo), then tries each
 * address in order within one timeout budget, each attempt a blocking connect.
 * Here: resolve on the worker pool (same hints as php_network_getaddresses, so
 * the same order), then for each address hand the ORIGINAL connect that single IP
 * literal in async mode — PHP still creates the socket with every context option
 * (bindto, tcp_nodelay, keepalive, linger, buffers) — and wait for writability via
 * the write handler with the remaining budget. Errors are recorded exactly as the
 * socket layer does, so the calling PHP function emits its own native warning. A
 * failed lookup falls back to the native connect so its error message is PHP's. */
static int phasync_ipv6_usable(void)
{
	static int usable = -1;       /* php_network_getaddresses' ipv6_borked probe */
	if (usable == -1) {
		int s6 = socket(AF_INET6, SOCK_DGRAM, 0);
		usable = s6 >= 0;
		if (s6 >= 0) {
			close(s6);
		}
	}
	return usable;
}

static int phasync_connect_cooperative(php_stream *stream, php_stream_xport_param *xp)
{
	php_netstream_data_t *sock = phasync_sock_data(stream);
	char *name = xp->inputs.name, host[256], literal[INET6_ADDRSTRLEN + 16];
	size_t namelen = xp->inputs.namelen, hostlen, portlen;
	const char *port;
	struct timeval *tv = xp->inputs.timeout;
	double deadline;
	struct addrinfo hints, *res = NULL, *ai;
	unsigned char bin[sizeof(struct in6_addr)];
	phasync_hook_entry *e;
	int attempted = 0;

	if (sock == NULL || name == NULL || namelen == 0) {
		return phasync_orig_set_option(stream, PHP_STREAM_OPTION_XPORT_API, 0, xp);
	}
	/* "host:port" or "[v6]:port", as xp_socket's parse_ip_address reads it */
	if (name[0] == '[') {
		const char *end = memchr(name, ']', namelen);
		if (!end || end + 1 >= name + namelen || end[1] != ':') {
			return phasync_orig_set_option(stream, PHP_STREAM_OPTION_XPORT_API, 0, xp);
		}
		hostlen = end - name - 1;
		memcpy(host, name + 1, MIN(hostlen, sizeof(host) - 1));
		port = end + 2;
	} else {
		const char *colon = zend_memrchr(name, ':', namelen);
		if (!colon) {
			return phasync_orig_set_option(stream, PHP_STREAM_OPTION_XPORT_API, 0, xp);
		}
		hostlen = colon - name;
		memcpy(host, name, MIN(hostlen, sizeof(host) - 1));
		port = colon + 1;
	}
	if (hostlen >= sizeof(host)) {
		return phasync_orig_set_option(stream, PHP_STREAM_OPTION_XPORT_API, 0, xp);
	}
	host[hostlen] = '\0';
	portlen = name + namelen - port;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = phasync_ipv6_usable() ? AF_UNSPEC : AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	if (inet_pton(AF_INET, host, bin) == 1 || inet_pton(AF_INET6, host, bin) == 1) {
		hints.ai_flags = AI_NUMERICHOST;             /* no DNS to offload */
		if (getaddrinfo(host, NULL, &hints, &res) != 0) {
			res = NULL;
		}
	} else {
		phasync_task t;
		memset(&t, 0, sizeof(t));
		t.type  = PHASYNC_OP_GETADDRINFO;
		t.host  = host;
		t.hints = hints;
		phasync_pool_run(&t);
		if (EG(exception)) {
			if (t.ai) freeaddrinfo(t.ai);
			xp->outputs.returncode = -1;
			return PHP_STREAM_OPTION_RETURN_OK;       /* exception pending: propagates */
		}
		res = t.result == 0 ? t.ai : NULL;
		if (!res && t.ai) freeaddrinfo(t.ai);
	}
	if (res == NULL) {
		/* Lookup failed: let the native connect produce PHP's exact error. */
		return phasync_orig_set_option(stream, PHP_STREAM_OPTION_XPORT_API, 0, xp);
	}
	/* Like native, the connect budget starts after the lookup, not before it. */
	deadline = tv ? phasync_now() + (double) tv->tv_sec + (double) tv->tv_usec / 1000000.0 : 0;

	for (ai = res; ai; ai = ai->ai_next) {
		double remaining = tv ? deadline - phasync_now() : INFINITY;
		char ip[INET6_ADDRSTRLEN];
		int r;

		if (attempted && remaining <= 0) {
			break;                    /* native: no further attempts once time is up */
		}
		if (getnameinfo(ai->ai_addr, ai->ai_addrlen, ip, sizeof(ip), NULL, 0, NI_NUMERICHOST) != 0) {
			continue;
		}
		snprintf(literal, sizeof(literal), ai->ai_family == AF_INET6 ? "[%s]:%.*s" : "%s:%.*s",
			ip, (int) portlen, port);
		if (xp->outputs.error_text) {  /* native frees the previous attempt's error */
			zend_string_release(xp->outputs.error_text);
			xp->outputs.error_text = NULL;
		}
		xp->inputs.name = literal;
		xp->inputs.namelen = strlen(literal);
		xp->op = STREAM_XPORT_OP_CONNECT_ASYNC;
		r = phasync_orig_set_option(stream, PHP_STREAM_OPTION_XPORT_API, 0, xp);
		xp->op = STREAM_XPORT_OP_CONNECT;
		xp->inputs.name = name;
		xp->inputs.namelen = namelen;
		attempted = 1;
		if (r != PHP_STREAM_OPTION_RETURN_OK) {
			freeaddrinfo(res);
			return r;
		}
		if (xp->outputs.returncode == 1) {                 /* EINPROGRESS */
			int err = 0;
			socklen_t l = sizeof(err);
			/* Wait on a transient resource we own, never the stream's own: the stream
			 * is still being created, and its creator may not expect anyone else to
			 * hold it — mysqlnd frees its stream's zend_resource raw right after
			 * connecting (mysqlnd_fixup_regular_list), which would leave a handler's
			 * references dangling (heap corruption). */
			int w = remaining > 0
				? phasync_wait_fd(phasync_write_handler(), NULL, sock->socket, remaining)
				: PHASYNC_WAIT_TIMEOUT;
			if (w == PHASYNC_WAIT_ERROR) {
				close(sock->socket);
				sock->socket = -1;
				xp->outputs.returncode = -1;
				freeaddrinfo(res);
				return PHP_STREAM_OPTION_RETURN_OK;       /* exception pending: propagates */
			}
			if (w == PHASYNC_WAIT_TIMEOUT) {
				err = ETIMEDOUT;
			} else if (getsockopt(sock->socket, SOL_SOCKET, SO_ERROR, &err, &l) != 0) {
				err = errno;
			}
			if (err == 0) {
				xp->outputs.returncode = 0;
			} else {
				close(sock->socket);
				sock->socket = -1;
				xp->outputs.returncode = -1;
				xp->outputs.error_code = err;
				if (xp->want_errortext) {
					xp->outputs.error_text = php_socket_error_str(err);
				}
				if (w == PHASYNC_WAIT_TIMEOUT) {
					break;
				}
				continue;
			}
		}
		if (xp->outputs.returncode == 0) {
			/* Connected. The native sync path leaves the socket blocking. */
			int fl = fcntl(sock->socket, F_GETFL, 0);
			if (fl != -1) {
				fcntl(sock->socket, F_SETFL, fl & ~O_NONBLOCK);
			}
			xp->outputs.error_code = 0;
			if (xp->outputs.error_text) {
				zend_string_release(xp->outputs.error_text);
				xp->outputs.error_text = NULL;
			}
			if ((e = phasync_entry(stream))) {
				e->applied = -1;       /* a new fd: re-apply our mode on next I/O */
			}
			freeaddrinfo(res);
			return PHP_STREAM_OPTION_RETURN_OK;
		}
		/* -1: failed at once; the socket layer closed it and recorded the error */
	}
	freeaddrinfo(res);
	xp->outputs.returncode = -1;
	return PHP_STREAM_OPTION_RETURN_OK;
}

/* Record the caller's intended blocking mode and pass it through, so the native
 * op (used outside a scope) sees the real intent while we still know it. */
static int phasync_wrapped_set_option(php_stream *stream, int option, int value, void *ptrparam)
{
	if (option == PHP_STREAM_OPTION_BLOCKING) {
		phasync_hook_entry *e = phasync_entry_ensure(stream);
		e->want_block = (value != 0);
		e->applied    = -1;   /* orig is about to change the fd; re-apply on next I/O */
	} else if (option == PHP_STREAM_OPTION_XPORT_API && ptrparam
	        && ((php_stream_xport_param *) ptrparam)->op == STREAM_XPORT_OP_ACCEPT
	        && phasync_read_handler() != NULL && EG(active_fiber) != NULL) {
		return phasync_accept_cooperative(stream, (php_stream_xport_param *) ptrparam);
	} else if (option == PHP_STREAM_OPTION_XPORT_API && ptrparam
	        && ((php_stream_xport_param *) ptrparam)->op == STREAM_XPORT_OP_CONNECT
	        && stream->ops->read == phasync_wrapped_read       /* not tls://: crypto follows */
	        && phasync_write_handler() != NULL && EG(active_fiber) != NULL
	        && (strcmp(PHASYNC_ORIG(stream)->label, "tcp_socket") == 0
	            || strcmp(PHASYNC_ORIG(stream)->label, "tcp_socket/ssl") == 0)) {
		return phasync_connect_cooperative(stream, (php_stream_xport_param *) ptrparam);
	}
	return phasync_orig_set_option(stream, option, value, ptrparam);
}

static int phasync_wrapped_close(php_stream *stream, int close_handle)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	phasync_hook_entry *e = phasync_entry(stream);

	if (e) {
		/* If we forced a detached fd non-blocking but the caller wanted blocking,
		 * hand it back the way they left it. */
		if (!close_handle && e->applied == 1 && e->want_block) {
			php_socket_t fd = phasync_stream_fd(stream);
			if (fd != -1) {
				int flags = fcntl(fd, F_GETFL, 0);
				if (flags != -1) {
					fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
				}
			}
		}
		zend_hash_index_del(&PHASYNC_G(hooked), (zend_ulong) (uintptr_t) stream);
	}
	return orig->close(stream, close_handle);
}

/* Build (or fetch cached) a wrapped ops for the given original ops: a copy with
 * read/write/close overridden and every other op left as the original's. */
static php_stream_ops *phasync_wrapped_ops_for(const php_stream_ops *orig, phasync_mode mode)
{
	/* One orig ops (e.g. stdio) can be wrapped in different modes (a regular file
	 * as POOL, a FIFO as RAW), so the cache key folds the mode into the low bits
	 * of the (pointer-aligned) orig address. */
	zend_ulong key = ((zend_ulong) (uintptr_t) orig) | (zend_ulong) mode;
	phasync_wops *w = zend_hash_index_find_ptr(&PHASYNC_G(wrapped_ops_cache), key);
	if (w) {
		return &w->ops;
	}
	w = pemalloc(sizeof(*w), 1);
	w->ops = *orig;
	switch (mode) {
		case PHASYNC_MODE_TLS:
			w->ops.read  = phasync_wrapped_read_tls;
			w->ops.write = phasync_wrapped_write_tls;
			w->ops.label = "phasync-wrapped-tls";
			break;
		case PHASYNC_MODE_POOL:
			w->ops.read  = phasync_wrapped_read_pool;
			w->ops.write = phasync_wrapped_write_pool;
			w->ops.label = "phasync-wrapped-pool";
			break;
		default:
			w->ops.read  = phasync_wrapped_read;
			w->ops.write = phasync_wrapped_write;
			w->ops.label = "phasync-wrapped";
			break;
	}
	w->ops.close = phasync_wrapped_close;
	w->ops.set_option = phasync_wrapped_set_option;
	w->orig = orig;
	zend_hash_index_add_ptr(&PHASYNC_G(wrapped_ops_cache), key, w);
	return &w->ops;
}

/* Wrap any descriptor-backed stream (socket or pipe). Ops replacement only;
 * fd/non-blocking setup is deferred to first I/O. */
static void phasync_wrap_stream(php_stream *stream, phasync_mode mode)
{
	if (stream == NULL || stream->ops == NULL) {
		return;
	}
	/* Already wrapped? (also covers accepted sockets that inherited wrapped ops) */
	if (stream->ops->read == phasync_wrapped_read
	 || stream->ops->read == phasync_wrapped_read_tls
	 || stream->ops->read == phasync_wrapped_read_pool) {
		return;
	}
	stream->ops = phasync_wrapped_ops_for(stream->ops, mode);
}

/* ---- transport factories ------------------------------------------------- */

static php_stream *phasync_tcp_factory(const char *proto, size_t protolen,
		const char *resourcename, size_t resourcenamelen, const char *persistent_id,
		int options, int flags, struct timeval *timeout,
		php_stream_context *context STREAMS_DC)
{
	php_stream *s = PHASYNC_G(orig_tcp)(proto, protolen, resourcename, resourcenamelen,
		persistent_id, options, flags, timeout, context STREAMS_CC);
	phasync_wrap_stream(s, PHASYNC_MODE_RAW);
	return s;
}

static php_stream *phasync_unix_factory(const char *proto, size_t protolen,
		const char *resourcename, size_t resourcenamelen, const char *persistent_id,
		int options, int flags, struct timeval *timeout,
		php_stream_context *context STREAMS_DC)
{
	php_stream *s = PHASYNC_G(orig_unix)(proto, protolen, resourcename, resourcenamelen,
		persistent_id, options, flags, timeout, context STREAMS_CC);
	phasync_wrap_stream(s, PHASYNC_MODE_RAW);
	return s;
}

static php_stream *phasync_ssl_factory(const char *proto, size_t protolen,
		const char *resourcename, size_t resourcenamelen, const char *persistent_id,
		int options, int flags, struct timeval *timeout,
		php_stream_context *context STREAMS_DC)
{
	php_stream *s = PHASYNC_G(orig_ssl)(proto, protolen, resourcename, resourcenamelen,
		persistent_id, options, flags, timeout, context STREAMS_CC);
	phasync_wrap_stream(s, PHASYNC_MODE_TLS);   /* TLS: delegate to SSL_read/SSL_write */
	return s;
}

/* The openssl extension registers one factory under several scheme names. */
static const char *phasync_ssl_schemes[] = {
	"ssl", "tls", "sslv3", "tlsv1.0", "tlsv1.1", "tlsv1.2", "tlsv1.3", NULL
};

/* ---- proc_open() override: wrap the pipe streams it produces -------------- */

static void phasync_wrap_pipes_array(zval *pipes)
{
	zval *elem;
	php_stream *stream;

	ZVAL_DEREF(pipes);
	if (Z_TYPE_P(pipes) != IS_ARRAY) {
		return;
	}
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(pipes), elem) {
		ZVAL_DEREF(elem);
		if (Z_TYPE_P(elem) != IS_RESOURCE) {
			continue;
		}
		stream = (php_stream *) zend_fetch_resource2_ex(elem, NULL,
			php_file_le_stream(), php_file_le_pstream());
		if (stream) {
			phasync_wrap_stream(stream, PHASYNC_MODE_RAW);
		}
	} ZEND_HASH_FOREACH_END();
}

static ZEND_NAMED_FUNCTION(phasync_proc_open_override)
{
	PHASYNC_G(orig_proc_open)(INTERNAL_FUNCTION_PARAM_PASSTHRU);

	if (!EG(exception) && ZEND_NUM_ARGS() >= 3) {
		/* arg #3 ($pipes) is by-ref and now holds the pipe stream resources */
		zval *pipes = ZEND_CALL_ARG(execute_data, 3);
		if (pipes) {
			phasync_wrap_pipes_array(pipes);
		}
	}
}

/* ---- sleep()/usleep() override ------------------------------------------- */

/* Seconds -> microseconds for the sleep handler, clamped: native PHP accepts
 * absurdly long sleeps, and the conversion must not overflow zend_long. */
static zend_long phasync_sec_to_usec(double sec)
{
	double us = sec * 1000000.0;
	return us >= (double) ZEND_LONG_MAX ? ZEND_LONG_MAX : (zend_long) us;
}

static ZEND_NAMED_FUNCTION(phasync_sleep_override)
{
	zend_long seconds;

	/* Only intercept when a fiber is actually running. The scheduler itself
	 * idle-waits by calling these same sleep functions from the main (non-fiber)
	 * context; delegating those to the handler (which can only Fiber::suspend
	 * inside a fiber) would collapse the wait to a no-op and spin the loop. */
	if (phasync_sleep_handler() == NULL || EG(active_fiber) == NULL) {
		PHASYNC_G(orig_sleep)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_LONG(seconds)
	ZEND_PARSE_PARAMETERS_END();

	if (seconds < 0) {
		zend_argument_value_error(1, "must be greater than or equal to 0");
		RETURN_THROWS();
	}
	phasync_call_sleep(phasync_sleep_handler(), phasync_sec_to_usec((double) seconds));
	RETURN_LONG(0);
}

static ZEND_NAMED_FUNCTION(phasync_usleep_override)
{
	zend_long usec;

	if (phasync_sleep_handler() == NULL || EG(active_fiber) == NULL) {
		PHASYNC_G(orig_usleep)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_LONG(usec)
	ZEND_PARSE_PARAMETERS_END();

	if (usec < 0) {
		zend_argument_value_error(1, "must be greater than or equal to 0");
		RETURN_THROWS();
	}
	phasync_call_sleep(phasync_sleep_handler(), usec);
}

static ZEND_NAMED_FUNCTION(phasync_time_nanosleep_override)
{
	zend_long sec, nsec;

	if (phasync_sleep_handler() == NULL || EG(active_fiber) == NULL) {
		PHASYNC_G(orig_time_nanosleep)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(2, 2)
		Z_PARAM_LONG(sec)
		Z_PARAM_LONG(nsec)
	ZEND_PARSE_PARAMETERS_END();

	if (sec < 0) {
		zend_argument_value_error(1, "must be greater than or equal to 0");
		RETURN_THROWS();
	}
	if (nsec < 0) {
		zend_argument_value_error(2, "must be greater than or equal to 0");
		RETURN_THROWS();
	}
	if (nsec > 999999999) {             /* native nanosleep() fails with EINVAL */
		zend_value_error("Nanoseconds was not in the range 0 to 999 999 999 or seconds was negative");
		RETURN_THROWS();
	}
	phasync_call_sleep(phasync_sleep_handler(),
		phasync_sec_to_usec((double) sec + (double) nsec / 1000000000.0));
	RETURN_TRUE;
}

static ZEND_NAMED_FUNCTION(phasync_time_sleep_until_override)
{
	double ts, now;
	struct timeval tv;
	zend_long usec;

	if (phasync_sleep_handler() == NULL || EG(active_fiber) == NULL) {
		PHASYNC_G(orig_time_sleep_until)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_DOUBLE(ts)
	ZEND_PARSE_PARAMETERS_END();

	gettimeofday(&tv, NULL);
	now = (double) tv.tv_sec + (double) tv.tv_usec / 1000000.0;
	usec = (ts > now) ? phasync_sec_to_usec(ts - now) : 0;
	phasync_call_sleep(phasync_sleep_handler(), usec);
	RETURN_TRUE;
}

static ZEND_NAMED_FUNCTION(phasync_gethostbyname_override)
{
	char *host;
	size_t hostlen;
	phasync_task t;
	char ip[INET_ADDRSTRLEN];

	if (phasync_read_handler() == NULL || EG(active_fiber) == NULL) {
		PHASYNC_G(orig_gethostbyname)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_PATH(host, hostlen)
	ZEND_PARSE_PARAMETERS_END();

	if (hostlen == 0 || hostlen > MAXFQDNLEN) {
		PHASYNC_G(orig_gethostbyname)(INTERNAL_FUNCTION_PARAM_PASSTHRU);   /* native warning */
		return;
	}
	memset(&t, 0, sizeof(t));
	t.type = PHASYNC_OP_GETHOSTBYNAME;
	t.host = host;
	phasync_pool_run(&t);   /* resolves on a pool thread; parks the fiber */

	if (t.hostok && t.naddrs > 0 && inet_ntop(AF_INET, &t.addrs[0], ip, sizeof(ip))) {
		RETURN_STRING(ip);
	}
	RETURN_STRINGL(host, hostlen);   /* native returns the input unchanged on failure */
}

/* gethostbynamel(): the same lookup as gethostbyname(), every address. */
static ZEND_NAMED_FUNCTION(phasync_gethostbynamel_override)
{
	char *host;
	size_t hostlen;
	phasync_task t;
	char ip[INET_ADDRSTRLEN];

	if (phasync_read_handler() == NULL || EG(active_fiber) == NULL) {
		PHASYNC_G(orig_gethostbynamel)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_PATH(host, hostlen)
	ZEND_PARSE_PARAMETERS_END();

	if (hostlen > MAXFQDNLEN) {
		PHASYNC_G(orig_gethostbynamel)(INTERNAL_FUNCTION_PARAM_PASSTHRU);  /* native warning */
		return;
	}
	memset(&t, 0, sizeof(t));
	t.type = PHASYNC_OP_GETHOSTBYNAME;
	t.host = host;
	phasync_pool_run(&t);
	if (EG(exception)) {
		return;
	}
	if (!t.hostok) {
		RETURN_FALSE;
	}
	array_init(return_value);
	for (int i = 0; i < t.naddrs; i++) {
		if (inet_ntop(AF_INET, &t.addrs[i], ip, sizeof(ip))) {
			add_next_index_string(return_value, ip);
		}
	}
}

#ifdef HAVE_FULL_DNS_FUNCS
/* dns_check_record() / checkdnsrr() — logic as PHP's (see the DNS section's
 * notice); the query runs on the pool. Argument errors go to the original so
 * their (version-specific) messages stay exact. */
static ZEND_NAMED_FUNCTION(phasync_dns_check_record_override)
{
	phasync_querybuf *answer;
	char *hostname;
	size_t hostname_len;
	zend_string *rectype = NULL;
	int type = DNS_T_MX, i, dns_errno, init_ok;

	if (phasync_read_handler() == NULL || EG(active_fiber) == NULL) {
		PHASYNC_G(orig_dns_check_record)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(1, 2)
		Z_PARAM_PATH(hostname, hostname_len)
		Z_PARAM_OPTIONAL
		Z_PARAM_STR(rectype)
	ZEND_PARSE_PARAMETERS_END();

	if (rectype) {
		if (zend_string_equals_literal_ci(rectype, "A")) type = DNS_T_A;
		else if (zend_string_equals_literal_ci(rectype, "NS")) type = DNS_T_NS;
		else if (zend_string_equals_literal_ci(rectype, "MX")) type = DNS_T_MX;
		else if (zend_string_equals_literal_ci(rectype, "PTR")) type = DNS_T_PTR;
		else if (zend_string_equals_literal_ci(rectype, "ANY")) type = DNS_T_ANY;
		else if (zend_string_equals_literal_ci(rectype, "SOA")) type = DNS_T_SOA;
		else if (zend_string_equals_literal_ci(rectype, "CAA")) type = DNS_T_CAA;
		else if (zend_string_equals_literal_ci(rectype, "TXT")) type = DNS_T_TXT;
		else if (zend_string_equals_literal_ci(rectype, "CNAME")) type = DNS_T_CNAME;
		else if (zend_string_equals_literal_ci(rectype, "AAAA")) type = DNS_T_AAAA;
		else if (zend_string_equals_literal_ci(rectype, "SRV")) type = DNS_T_SRV;
		else if (zend_string_equals_literal_ci(rectype, "NAPTR")) type = DNS_T_NAPTR;
		else if (zend_string_equals_literal_ci(rectype, "A6")) type = DNS_T_A6;
		else type = -1;
	}
	if (hostname_len == 0 || type == -1) {
		PHASYNC_G(orig_dns_check_record)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}

	answer = ecalloc(1, sizeof(*answer));
	i = phasync_dns_search(hostname, type, answer, &dns_errno, &init_ok);
	if (EG(exception) || !init_ok || i < 0) {
		efree(answer);
		if (!EG(exception)) {
			RETVAL_FALSE;
		}
		return;
	}
	RETVAL_BOOL(ntohs(((HEADER *) answer)->ancount) != 0);
	efree(answer);
}

/* dns_get_record() — logic as PHP's; each per-type query runs on the pool. */
static ZEND_NAMED_FUNCTION(phasync_dns_get_record_override)
{
	char *hostname;
	size_t hostname_len;
	zend_long type_param = PHP_DNS_ANY;
	zval *authns = NULL, *addtl = NULL;
	int type_to_fetch;
	int dns_errno, init_ok;
	HEADER *hp;
	phasync_querybuf *answer;
	uint8_t *cp = NULL, *end = NULL;
	int n, qd, an, ns = 0, ar = 0;
	int type, first_query = 1, store_results = 1;
	bool raw = 0;

	if (phasync_read_handler() == NULL || EG(active_fiber) == NULL) {
		PHASYNC_G(orig_dns_get_record)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(1, 5)
		Z_PARAM_PATH(hostname, hostname_len)
		Z_PARAM_OPTIONAL
		Z_PARAM_LONG(type_param)
		Z_PARAM_ZVAL(authns)
		Z_PARAM_ZVAL(addtl)
		Z_PARAM_BOOL(raw)
	ZEND_PARSE_PARAMETERS_END();

	/* Invalid types are rejected by the original, with its own message. */
	if ((!raw && (type_param & ~PHP_DNS_ALL) && type_param != PHP_DNS_ANY)
	 || (raw && (type_param < 1 || type_param > 0xFFFF))) {
		PHASYNC_G(orig_dns_get_record)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	if (authns) {
		authns = zend_try_array_init(authns);
		if (!authns) {
			RETURN_THROWS();
		}
	}
	if (addtl) {
		addtl = zend_try_array_init(addtl);
		if (!addtl) {
			RETURN_THROWS();
		}
	}

	array_init(return_value);
	answer = emalloc(sizeof(*answer));

	if (raw) {
		type = -1;
	} else if (type_param == PHP_DNS_ANY) {
		type = PHP_DNS_NUM_TYPES + 1;
	} else {
		type = 0;
	}

	for ( ;
		type < (addtl ? (PHP_DNS_NUM_TYPES + 2) : PHP_DNS_NUM_TYPES) || first_query;
		type++
	) {
		first_query = 0;
		switch (type) {
			case -1:
				type_to_fetch = type_param;
				type = PHP_DNS_NUM_TYPES - 1;
				break;
			case 0:  type_to_fetch = type_param&PHP_DNS_A     ? DNS_T_A     : 0; break;
			case 1:  type_to_fetch = type_param&PHP_DNS_NS    ? DNS_T_NS    : 0; break;
			case 2:  type_to_fetch = type_param&PHP_DNS_CNAME ? DNS_T_CNAME : 0; break;
			case 3:  type_to_fetch = type_param&PHP_DNS_SOA   ? DNS_T_SOA   : 0; break;
			case 4:  type_to_fetch = type_param&PHP_DNS_PTR   ? DNS_T_PTR   : 0; break;
			case 5:  type_to_fetch = type_param&PHP_DNS_HINFO ? DNS_T_HINFO : 0; break;
			case 6:  type_to_fetch = type_param&PHP_DNS_MX    ? DNS_T_MX    : 0; break;
			case 7:  type_to_fetch = type_param&PHP_DNS_TXT   ? DNS_T_TXT   : 0; break;
			case 8:  type_to_fetch = type_param&PHP_DNS_AAAA  ? DNS_T_AAAA  : 0; break;
			case 9:  type_to_fetch = type_param&PHP_DNS_SRV   ? DNS_T_SRV   : 0; break;
			case 10: type_to_fetch = type_param&PHP_DNS_NAPTR ? DNS_T_NAPTR : 0; break;
			case 11: type_to_fetch = type_param&PHP_DNS_A6    ? DNS_T_A6    : 0; break;
			case 12: type_to_fetch = type_param&PHP_DNS_CAA   ? DNS_T_CAA   : 0; break;
			case PHP_DNS_NUM_TYPES:
				store_results = 0;
				continue;
			default:
			case (PHP_DNS_NUM_TYPES + 1):
				type_to_fetch = DNS_T_ANY;
				break;
		}

		if (type_to_fetch) {
			memset(answer, 0, sizeof(*answer));
			n = phasync_dns_search(hostname, type_to_fetch, answer, &dns_errno, &init_ok);
			if (EG(exception)) {
				efree(answer);
				return;
			}
			if (!init_ok) {
				efree(answer);
				zend_array_destroy(Z_ARR_P(return_value));
				RETURN_FALSE;
			}
			if (n < 0) {
				switch (dns_errno) {
					case NO_DATA:
					case HOST_NOT_FOUND:
						continue;
					case NO_RECOVERY:
						php_error_docref(NULL, E_WARNING, "An unexpected server failure occurred.");
						break;
					case TRY_AGAIN:
						php_error_docref(NULL, E_WARNING, "A temporary server error occurred.");
						break;
					default:
						php_error_docref(NULL, E_WARNING, "DNS Query failed");
				}
				efree(answer);
				zend_array_destroy(Z_ARR_P(return_value));
				RETURN_FALSE;
			}

			cp = answer->qb2 + HFIXEDSZ;
			end = answer->qb2 + n;
			hp = (HEADER *) answer;
			qd = ntohs(hp->qdcount);
			an = ntohs(hp->ancount);
			ns = ntohs(hp->nscount);
			ar = ntohs(hp->arcount);

			while (qd-- > 0) {
				n = dn_skipname(cp, end);
				if (n < 0) {
					php_error_docref(NULL, E_WARNING, "Unable to parse DNS data received");
					efree(answer);
					zend_array_destroy(Z_ARR_P(return_value));
					RETURN_FALSE;
				}
				cp += n + QFIXEDSZ;
			}

			while (an-- && cp && cp < end) {
				zval retval;
				cp = phasync_dns_parserr(cp, end, answer, type_to_fetch, store_results, raw, &retval);
				if (Z_TYPE(retval) != IS_UNDEF && store_results) {
					add_next_index_zval(return_value, &retval);
				}
			}

			if (authns || addtl) {
				while (ns-- > 0 && cp && cp < end) {
					zval retval;
					cp = phasync_dns_parserr(cp, end, answer, DNS_T_ANY, authns != NULL, raw, &retval);
					if (Z_TYPE(retval) != IS_UNDEF) {
						add_next_index_zval(authns, &retval);
					}
				}
			}

			if (addtl) {
				while (ar-- > 0 && cp && cp < end) {
					zval retval;
					cp = phasync_dns_parserr(cp, end, answer, DNS_T_ANY, 1, raw, &retval);
					if (Z_TYPE(retval) != IS_UNDEF) {
						add_next_index_zval(addtl, &retval);
					}
				}
			}
		}
	}
	efree(answer);
}

/* dns_get_mx() / getmxrr() — logic as PHP's; the query runs on the pool. */
static ZEND_NAMED_FUNCTION(phasync_dns_get_mx_override)
{
	char *hostname;
	size_t hostname_len;
	zval *mx_list, *weight_list = NULL;
	int count, qdc, dns_errno, init_ok;
	u_short type, weight;
	phasync_querybuf *answer;
	char buf[PHASYNC_DNS_MAXHOSTNAMELEN] = {0};
	HEADER *hp;
	uint8_t *cp, *end;
	int i;

	if (phasync_read_handler() == NULL || EG(active_fiber) == NULL) {
		PHASYNC_G(orig_dns_get_mx)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(2, 3)
		Z_PARAM_PATH(hostname, hostname_len)
		Z_PARAM_ZVAL(mx_list)
		Z_PARAM_OPTIONAL
		Z_PARAM_ZVAL(weight_list)
	ZEND_PARSE_PARAMETERS_END();

	mx_list = zend_try_array_init(mx_list);
	if (!mx_list) {
		RETURN_THROWS();
	}
	if (weight_list) {
		weight_list = zend_try_array_init(weight_list);
		if (!weight_list) {
			RETURN_THROWS();
		}
	}

	answer = ecalloc(1, sizeof(*answer));
	i = phasync_dns_search(hostname, DNS_T_MX, answer, &dns_errno, &init_ok);
	if (EG(exception) || !init_ok || i < 0) {
		efree(answer);
		if (!EG(exception)) {
			RETVAL_FALSE;
		}
		return;
	}
	hp = (HEADER *) answer;
	cp = answer->qb2 + HFIXEDSZ;
	end = answer->qb2 + i;
	for (qdc = ntohs((unsigned short)hp->qdcount); qdc--; cp += i + QFIXEDSZ) {
		if ((i = dn_skipname(cp, end)) < 0 ) {
			efree(answer);
			RETURN_FALSE;
		}
	}
	count = ntohs((unsigned short)hp->ancount);
	while (--count >= 0 && cp < end) {
		if ((i = dn_skipname(cp, end)) < 0 ) {
			efree(answer);
			RETURN_FALSE;
		}
		cp += i;
		GETSHORT(type, cp);
		cp += INT16SZ + INT32SZ;
		GETSHORT(i, cp);
		if (type != DNS_T_MX) {
			cp += i;
			continue;
		}
		GETSHORT(weight, cp);
		if ((i = dn_expand(answer->qb2, end, cp, buf, sizeof(buf)-1)) < 0) {
			efree(answer);
			RETURN_FALSE;
		}
		cp += i;
		add_next_index_string(mx_list, buf);
		if (weight_list) {
			add_next_index_long(weight_list, weight);
		}
	}
	efree(answer);
	RETURN_BOOL(zend_hash_num_elements(Z_ARRVAL_P(mx_list)) != 0);
}
#endif /* HAVE_FULL_DNS_FUNCS */

/* gethostbyaddr(): the reverse lookup (getnameinfo, NI_NAMEREQD) on the pool. */
static ZEND_NAMED_FUNCTION(phasync_gethostbyaddr_override)
{
	char *addr;
	size_t addrlen;
	phasync_task t;

	if (phasync_read_handler() == NULL || EG(active_fiber) == NULL) {
		PHASYNC_G(orig_gethostbyaddr)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_PATH(addr, addrlen)
	ZEND_PARSE_PARAMETERS_END();

	memset(&t, 0, sizeof(t));
	{
		struct sockaddr_in6 *sa6 = (struct sockaddr_in6 *) &t.sa;
		struct sockaddr_in  *sa4 = (struct sockaddr_in *) &t.sa;
		if (inet_pton(AF_INET6, addr, &sa6->sin6_addr) == 1) {   /* same order as native */
			sa6->sin6_family = AF_INET6;
			t.salen = sizeof(*sa6);
		} else if (inet_pton(AF_INET, addr, &sa4->sin_addr) == 1) {
			sa4->sin_family = AF_INET;
			t.salen = sizeof(*sa4);
		} else {
			PHASYNC_G(orig_gethostbyaddr)(INTERNAL_FUNCTION_PARAM_PASSTHRU);  /* native warning */
			return;
		}
	}
	t.type = PHASYNC_OP_NAMEINFO;
	phasync_pool_run(&t);
	if (EG(exception)) {
		return;
	}
	if (t.hostok) {
		RETURN_STRING(t.hostresult);
	}
	RETURN_STRINGL(addr, addrlen);   /* native: no name -> the address itself */
}

/* Translate an fopen() mode string to open(2) flags (for the FIFO fast path). */
static int phasync_mode_to_oflags(const char *mode)
{
	int flags;
	bool plus = strchr(mode, '+') != NULL;

	switch (mode[0]) {
		case 'r': flags = plus ? O_RDWR : O_RDONLY; break;
		case 'w': flags = (plus ? O_RDWR : O_WRONLY) | O_CREAT | O_TRUNC; break;
		case 'a': flags = (plus ? O_RDWR : O_WRONLY) | O_CREAT | O_APPEND; break;
		case 'x': flags = (plus ? O_RDWR : O_WRONLY) | O_CREAT | O_EXCL; break;
		case 'c': flags = (plus ? O_RDWR : O_WRONLY) | O_CREAT; break;
		default:  flags = O_RDONLY; break;
	}
	return flags;
}

/* fopen() override.
 *   - Named pipe (FIFO): open() itself blocks in the kernel rendezvous until a
 *     peer opens the other end — before any fd exists, so there is nothing for a
 *     cooperative scheduler to poll and single-threaded code deadlocks. We run
 *     the open() on a dedicated thread so the fiber can yield until the peer
 *     shows up, then RAW-wrap the fd (after open, a FIFO honors O_NONBLOCK).
 *   - Regular file: open normally, then POOL-wrap so fread/fwrite offload to the
 *     worker pool (regular files are never readiness-pollable).
 *   - Everything else (wrappers, char/block devices, dirs): untouched. */
static ZEND_NAMED_FUNCTION(phasync_fopen_override)
{
	zend_string *filename, *mode;
	bool use_include_path = 0;
	zval *zcontext = NULL;
	php_stream *stream;
	struct stat st;

	if (phasync_read_handler() == NULL) {
		PHASYNC_G(orig_fopen)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}

	ZEND_PARSE_PARAMETERS_START(2, 4)
		Z_PARAM_STR(filename)
		Z_PARAM_STR(mode)
		Z_PARAM_OPTIONAL
		Z_PARAM_BOOL(use_include_path)
		Z_PARAM_RESOURCE_OR_NULL(zcontext)
	ZEND_PARSE_PARAMETERS_END();

	/* php://stdin|stdout|stderr and php://fd/N are backed by real OS descriptors,
	 * so wrap them RAW to make them cooperative too. Other php:// streams
	 * (memory/temp/input/output) and every other wrapper are not fd-backed and
	 * fall through untouched below. */
	if (zcontext == NULL && !use_include_path
	 && (strncasecmp(ZSTR_VAL(filename), "php://std", sizeof("php://std") - 1) == 0
	  || strncasecmp(ZSTR_VAL(filename), "php://fd/", sizeof("php://fd/") - 1) == 0)) {
		PHASYNC_G(orig_fopen)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		if (Z_TYPE_P(return_value) == IS_RESOURCE) {
			php_stream *s = NULL;
			php_stream_from_zval_no_verify(s, return_value);
			if (s && phasync_stream_fd(s) != -1) {
				phasync_wrap_stream(s, PHASYNC_MODE_RAW);
			}
		}
		return;
	}

	/* Only plain local paths get special handling; wrappers (http://, php://,
	 * data:// …) and include-path/context lookups fall straight through. */
	if (zcontext != NULL || use_include_path
	 || ZSTR_LEN(filename) == 0 || ZSTR_LEN(filename) >= PATH_MAX
	 || strstr(ZSTR_VAL(filename), "://") != NULL) {
		PHASYNC_G(orig_fopen)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}

	/* FIFO? Detect before opening — the open() is what blocks. */
	if (stat(ZSTR_VAL(filename), &st) == 0 && S_ISFIFO(st.st_mode)) {
		phasync_task t;
		memset(&t, 0, sizeof(t));
		t.type   = PHASYNC_OP_OPEN;
		t.fd     = -1;   /* so a cancelled (never-assigned) open never closes fd 0 */
		t.oflags = phasync_mode_to_oflags(ZSTR_VAL(mode));
		t.omode  = 0666;
		memcpy(t.path, ZSTR_VAL(filename), ZSTR_LEN(filename) + 1);

		phasync_pool_run_dedicated(&t);   /* blocks on its own thread, fiber yields */

		if (t.fd < 0) {
			php_error_docref(NULL, E_WARNING, "fopen(%s): %s",
				ZSTR_VAL(filename), strerror(t.err));
			RETURN_FALSE;
		}
		stream = php_stream_fopen_from_fd(t.fd, ZSTR_VAL(mode), NULL);
		if (!stream) {
			close(t.fd);
			RETURN_FALSE;
		}
		phasync_wrap_stream(stream, PHASYNC_MODE_RAW);
		php_stream_to_zval(stream, return_value);
		return;
	}

	/* Not a FIFO: open normally, then POOL-wrap regular files. */
	PHASYNC_G(orig_fopen)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
	if (Z_TYPE_P(return_value) == IS_RESOURCE) {
		php_stream *s = NULL;
		php_stream_from_zval_no_verify(s, return_value);
		if (s) {
			php_socket_t fd = phasync_stream_fd(s);
			if (fd != -1 && fstat(fd, &st) == 0 && S_ISREG(st.st_mode)) {
				phasync_wrap_stream(s, PHASYNC_MODE_POOL);
			}
		}
	}
}

/* stream_socket_pair() builds its sockets with socketpair(2) via
 * php_stream_sock_open_from_socket(), bypassing the transport factory — so wrap
 * both returned streams here (RAW; they are ordinary sockets). Wrapped
 * unconditionally, like the factory path: the wrapper is inert outside a scope. */
static ZEND_NAMED_FUNCTION(phasync_stream_socket_pair_override)
{
	PHASYNC_G(orig_stream_socket_pair)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
	if (Z_TYPE_P(return_value) == IS_ARRAY) {
		zval *el;
		ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(return_value), el) {
			if (Z_TYPE_P(el) == IS_RESOURCE) {
				php_stream *s = NULL;
				php_stream_from_zval_no_verify(s, el);
				if (s) {
					phasync_wrap_stream(s, PHASYNC_MODE_RAW);
				}
			}
		} ZEND_HASH_FOREACH_END();
	}
}

/* ---- stream_select() / socket_select() inside a scope --------------------
 *
 * A library's own stream_select()/socket_select() with a timeout would block the
 * whole process. Inside a scope, in a fiber, the call instead:
 *   1. probes the ORIGINAL function with a zero timeout, so every result (arrays,
 *      count, buffered-data emulation, warnings) is exactly native;
 *   2. if nothing is ready, registers all the descriptors in an epoll instance —
 *      itself a pollable fd, readable when any of them is — and waits on it via
 *      the read handler with the remaining timeout;
 *   3. probes again; a handler timeout means the select timed out (native: 0, all
 *      arrays emptied). */

static php_socket_t phasync_select_fd_stream(zval *elem)
{
	php_stream *s = NULL;
	php_socket_t fd = -1;

	ZVAL_DEREF(elem);
	if (Z_TYPE_P(elem) != IS_RESOURCE) {
		return -1;
	}
	php_stream_from_zval_no_verify(s, elem);
	if (s == NULL || php_stream_cast(s, PHP_STREAM_AS_FD_FOR_SELECT | PHP_STREAM_CAST_INTERNAL,
			(void *) &fd, 0) != SUCCESS) {
		return -1;
	}
	return fd;
}

/* ext/sockets' php_socket, mirrored here so socket_select() support doesn't depend
 * on its header, which PHP builds (e.g. the official Docker images) often don't
 * install. The layout is identical from PHP 8.2 to master. */
typedef struct {
	int         bsd_socket;
	int         type;
	int         error;
	int         blocking;
	zval        zstream;
	zend_object std;
} phasync_php_socket;

static php_socket_t phasync_select_fd_socket(zval *elem)
{
	struct stat st;
	int fd;

	ZVAL_DEREF(elem);
	/* Matched by name rather than socket_ce, so ext/sockets stays optional. */
	if (Z_TYPE_P(elem) != IS_OBJECT || !zend_string_equals_literal(Z_OBJCE_P(elem)->name, "Socket")) {
		return -1;
	}
	fd = ((phasync_php_socket *) ((char *) Z_OBJ_P(elem) - XtOffsetOf(phasync_php_socket, std)))->bsd_socket;
	/* Safety net should a future layout differ: only ever use a real socket fd. */
	return (fd >= 0 && fstat(fd, &st) == 0 && S_ISSOCK(st.st_mode)) ? fd : -1;
}

/* An epoll fd watching every descriptor in the three sets (read/write/except),
 * or -1 if none could be registered. */
static int phasync_select_epoll(zval *sets[3], php_socket_t (*fd_of)(zval *))
{
	static const uint32_t ev[3] = { EPOLLIN, EPOLLOUT, EPOLLPRI };
	HashTable events;
	zval *elem;
	int epfd, i, n = 0;

	zend_hash_init(&events, 8, NULL, NULL, 0);
	for (i = 0; i < 3; i++) {
		if (Z_TYPE_P(sets[i]) != IS_ARRAY) {
			continue;
		}
		ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(sets[i]), elem) {
			php_socket_t fd = fd_of(elem);
			zval *cur;
			if (fd < 0) {
				continue;
			}
			if ((cur = zend_hash_index_find(&events, (zend_ulong) fd))) {
				Z_LVAL_P(cur) |= ev[i];
			} else {
				zval z;
				ZVAL_LONG(&z, ev[i]);
				zend_hash_index_add_new(&events, (zend_ulong) fd, &z);
			}
		} ZEND_HASH_FOREACH_END();
	}
	epfd = epoll_create1(EPOLL_CLOEXEC);
	if (epfd >= 0) {
		zend_ulong fd;
		zval *e;
		ZEND_HASH_FOREACH_NUM_KEY_VAL(&events, fd, e) {
			struct epoll_event ee = { .events = (uint32_t) Z_LVAL_P(e), .data.fd = (int) fd };
			if (epoll_ctl(epfd, EPOLL_CTL_ADD, (int) fd, &ee) == 0) {
				n++;          /* regular files can't be registered (EPERM): always ready */
			}
		} ZEND_HASH_FOREACH_END();
		if (n == 0) {
			close(epfd);
			epfd = -1;
		}
	}
	zend_hash_destroy(&events);
	return epfd;
}

static void phasync_select_common(INTERNAL_FUNCTION_PARAMETERS, const char *fname,
		void (*orig)(INTERNAL_FUNCTION_PARAMETERS), php_socket_t (*fd_of)(zval *))
{
	zval *sets[3], saved[3], fn;
	zend_long sec = 0, usec = 0;
	bool secnull = 1, usecnull = 1;
	double deadline = 0;
	int i;

	if (PHASYNC_G(select_depth) || phasync_read_handler() == NULL || EG(active_fiber) == NULL) {
		orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(4, 5)
		Z_PARAM_ARRAY_EX2(sets[0], 1, 1, 0)
		Z_PARAM_ARRAY_EX2(sets[1], 1, 1, 0)
		Z_PARAM_ARRAY_EX2(sets[2], 1, 1, 0)
		Z_PARAM_LONG_OR_NULL(sec, secnull)
		Z_PARAM_OPTIONAL
		Z_PARAM_LONG_OR_NULL(usec, usecnull)
	ZEND_PARSE_PARAMETERS_END();

	/* A zero timeout never waits, and anything the original would reject (negative
	 * values, a null that isn't allowed) must fail exactly as it does natively. */
	if ((!secnull && sec == 0 && (usecnull || usec == 0))
	 || (!secnull && sec < 0) || (!usecnull && usec < 0)
	 || (secnull && !usecnull && usec != 0)
	 || (ZEND_NUM_ARGS() >= 5 && Z_TYPE_P(ZEND_CALL_ARG(execute_data, 5)) == IS_NULL
	     && strcmp(fname, "socket_select") == 0)) {
		orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	if (!secnull) {
		deadline = phasync_now() + (double) sec + (double) (usecnull ? 0 : usec) / 1000000.0;
	}

	for (i = 0; i < 3; i++) {
		if (sets[i]) {
			ZVAL_COPY(&saved[i], sets[i]);
		} else {
			ZVAL_NULL(&saved[i]);
		}
	}
	ZVAL_STRING(&fn, fname);

	for (;;) {
		zval args[5], ret, tmp;
		zval *savedp[3] = { &saved[0], &saved[1], &saved[2] };
		double remaining;
		int epfd, w;

		/* 1. zero-timeout probe of the original, on copies of the original arrays */
		for (i = 0; i < 3; i++) {
			ZVAL_COPY(&tmp, &saved[i]);
			ZVAL_NEW_REF(&args[i], &tmp);
		}
		ZVAL_LONG(&args[3], 0);
		ZVAL_LONG(&args[4], 0);
		ZVAL_UNDEF(&ret);
		PHASYNC_G(select_depth)++;
		call_user_function(NULL, NULL, &fn, &ret, 5, args);
		PHASYNC_G(select_depth)--;

		if (EG(exception) || Z_TYPE(ret) != IS_LONG || Z_LVAL(ret) != 0) {
			if (!EG(exception)) {
				for (i = 0; i < 3; i++) {
					if (sets[i]) {
						zval_ptr_dtor(sets[i]);
						ZVAL_COPY(sets[i], Z_REFVAL(args[i]));
					}
				}
				ZVAL_COPY(return_value, &ret);
			}
			for (i = 0; i < 5; i++) zval_ptr_dtor(&args[i]);
			zval_ptr_dtor(&ret);
			break;
		}
		for (i = 0; i < 5; i++) zval_ptr_dtor(&args[i]);
		zval_ptr_dtor(&ret);

		remaining = secnull ? INFINITY : deadline - phasync_now();
		if (remaining <= 0) {
			w = PHASYNC_WAIT_TIMEOUT;
		} else {
			/* 2. wait for any of them via one pollable epoll fd */
			epfd = phasync_select_epoll(savedp, fd_of);
			if (epfd < 0) {
				/* nothing pollable to wait on: fall back to the native call */
				for (i = 0; i < 3; i++) zval_ptr_dtor(&saved[i]);
				zval_ptr_dtor(&fn);
				orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
				return;
			}
			w = phasync_wait_fd(phasync_read_handler(), NULL, epfd, remaining);
			close(epfd);
		}
		if (w == PHASYNC_WAIT_TIMEOUT) {
			for (i = 0; i < 3; i++) {       /* native timeout: 0, every array emptied */
				if (sets[i]) {
					zval_ptr_dtor(sets[i]);
					ZVAL_EMPTY_ARRAY(sets[i]);
				}
			}
			RETVAL_LONG(0);
			break;
		}
		if (w == PHASYNC_WAIT_ERROR) {
			break;                           /* exception pending: propagate */
		}
		/* 3. ready (or spurious): probe again */
	}
	for (i = 0; i < 3; i++) zval_ptr_dtor(&saved[i]);
	zval_ptr_dtor(&fn);
}

static ZEND_NAMED_FUNCTION(phasync_stream_select_override)
{
	phasync_select_common(INTERNAL_FUNCTION_PARAM_PASSTHRU, "stream_select",
		PHASYNC_G(orig_stream_select), phasync_select_fd_stream);
}

static ZEND_NAMED_FUNCTION(phasync_socket_select_override)
{
	phasync_select_common(INTERNAL_FUNCTION_PARAM_PASSTHRU, "socket_select",
		PHASYNC_G(orig_socket_select), phasync_select_fd_socket);
}

/* ---- enable_hooks / disable_hooks ---------------------------------------- */

static zend_internal_function *phasync_find_ifunc(const char *name, size_t len)
{
	zend_function *f = zend_hash_str_find_ptr(CG(function_table), name, len);
	if (f && f->type == ZEND_INTERNAL_FUNCTION) {
		return &f->internal_function;
	}
	return NULL;
}

/* Install the transport re-registration + internal-function overrides. Called
 * lazily on the first manage() of a request and left in place: the hooks are
 * inert while no scope is active (the wrapped ops and sleep/pool overrides fall
 * through to the original when scope_top is NULL), so there is nothing to toggle
 * off. Idempotent. */
static void phasync_install_hooks(void)
{
	HashTable *xhash;
	zend_internal_function *f;

	if (PHASYNC_G(hooks_installed)) {
		return;
	}

	xhash = php_stream_xport_get_hash();
	PHASYNC_G(orig_tcp)  = zend_hash_str_find_ptr(xhash, "tcp", sizeof("tcp") - 1);
	PHASYNC_G(orig_unix) = zend_hash_str_find_ptr(xhash, "unix", sizeof("unix") - 1);
	if (PHASYNC_G(orig_tcp))  php_stream_xport_register("tcp", phasync_tcp_factory);
	if (PHASYNC_G(orig_unix)) php_stream_xport_register("unix", phasync_unix_factory);

	PHASYNC_G(orig_ssl) = zend_hash_str_find_ptr(xhash, "ssl", sizeof("ssl") - 1);
	if (PHASYNC_G(orig_ssl)) {
		const char **scheme;
		for (scheme = phasync_ssl_schemes; *scheme; scheme++) {
			if (zend_hash_str_exists(xhash, *scheme, strlen(*scheme))) {
				php_stream_xport_register(*scheme, phasync_ssl_factory);
			}
		}
	}

	if ((f = phasync_find_ifunc("proc_open", sizeof("proc_open") - 1))) {
		PHASYNC_G(orig_proc_open) = f->handler;
		f->handler = phasync_proc_open_override;
	}
	if ((f = phasync_find_ifunc("stream_socket_pair", sizeof("stream_socket_pair") - 1))) {
		PHASYNC_G(orig_stream_socket_pair) = f->handler;
		f->handler = phasync_stream_socket_pair_override;
	}
	if ((f = phasync_find_ifunc("stream_select", sizeof("stream_select") - 1))) {
		PHASYNC_G(orig_stream_select) = f->handler;
		f->handler = phasync_stream_select_override;
	}
	if ((f = phasync_find_ifunc("socket_select", sizeof("socket_select") - 1))) {
		PHASYNC_G(orig_socket_select) = f->handler;
		f->handler = phasync_socket_select_override;
	}
	if ((f = phasync_find_ifunc("sleep", sizeof("sleep") - 1))) {
		PHASYNC_G(orig_sleep) = f->handler;
		f->handler = phasync_sleep_override;
	}
	if ((f = phasync_find_ifunc("usleep", sizeof("usleep") - 1))) {
		PHASYNC_G(orig_usleep) = f->handler;
		f->handler = phasync_usleep_override;
	}
	if ((f = phasync_find_ifunc("time_nanosleep", sizeof("time_nanosleep") - 1))) {
		PHASYNC_G(orig_time_nanosleep) = f->handler;
		f->handler = phasync_time_nanosleep_override;
	}
	if ((f = phasync_find_ifunc("time_sleep_until", sizeof("time_sleep_until") - 1))) {
		PHASYNC_G(orig_time_sleep_until) = f->handler;
		f->handler = phasync_time_sleep_until_override;
	}
	if ((f = phasync_find_ifunc("gethostbyname", sizeof("gethostbyname") - 1))) {
		PHASYNC_G(orig_gethostbyname) = f->handler;
		f->handler = phasync_gethostbyname_override;
	}
	if ((f = phasync_find_ifunc("gethostbynamel", sizeof("gethostbynamel") - 1))) {
		PHASYNC_G(orig_gethostbynamel) = f->handler;
		f->handler = phasync_gethostbynamel_override;
	}
	if ((f = phasync_find_ifunc("gethostbyaddr", sizeof("gethostbyaddr") - 1))) {
		PHASYNC_G(orig_gethostbyaddr) = f->handler;
		f->handler = phasync_gethostbyaddr_override;
	}
#ifdef HAVE_FULL_DNS_FUNCS
	{
		/* checkdnsrr/getmxrr are separate function-table entries (aliases). */
		static const struct { const char *name; int which; } dnsfns[] = {
			{ "dns_check_record", 0 }, { "checkdnsrr", 0 },
			{ "dns_get_record", 1 },
			{ "dns_get_mx", 2 }, { "getmxrr", 2 },
		};
		for (size_t k = 0; k < sizeof(dnsfns) / sizeof(dnsfns[0]); k++) {
			if ((f = phasync_find_ifunc(dnsfns[k].name, strlen(dnsfns[k].name)))) {
				switch (dnsfns[k].which) {
					case 0: PHASYNC_G(orig_dns_check_record) = f->handler; f->handler = phasync_dns_check_record_override; break;
					case 1: PHASYNC_G(orig_dns_get_record)   = f->handler; f->handler = phasync_dns_get_record_override;   break;
					case 2: PHASYNC_G(orig_dns_get_mx)       = f->handler; f->handler = phasync_dns_get_mx_override;       break;
				}
			}
		}
	}
#endif
	if ((f = phasync_find_ifunc("fopen", sizeof("fopen") - 1))) {
		PHASYNC_G(orig_fopen) = f->handler;
		f->handler = phasync_fopen_override;
	}
	PHASYNC_G(hooks_installed) = 1;
}

static void phasync_restore_hooks(void)
{
	zend_internal_function *f;

	if (!PHASYNC_G(hooks_installed)) {
		return;
	}
	if (PHASYNC_G(orig_tcp))  php_stream_xport_register("tcp", PHASYNC_G(orig_tcp));
	if (PHASYNC_G(orig_unix)) php_stream_xport_register("unix", PHASYNC_G(orig_unix));
	if (PHASYNC_G(orig_ssl)) {
		HashTable *xh = php_stream_xport_get_hash();
		const char **scheme;
		for (scheme = phasync_ssl_schemes; *scheme; scheme++) {
			if (zend_hash_str_exists(xh, *scheme, strlen(*scheme))) {
				php_stream_xport_register(*scheme, PHASYNC_G(orig_ssl));
			}
		}
	}

	if (PHASYNC_G(orig_proc_open) && (f = phasync_find_ifunc("proc_open", sizeof("proc_open") - 1))) {
		f->handler = PHASYNC_G(orig_proc_open);
	}
	if (PHASYNC_G(orig_stream_socket_pair) && (f = phasync_find_ifunc("stream_socket_pair", sizeof("stream_socket_pair") - 1))) {
		f->handler = PHASYNC_G(orig_stream_socket_pair);
	}
	if (PHASYNC_G(orig_stream_select) && (f = phasync_find_ifunc("stream_select", sizeof("stream_select") - 1))) {
		f->handler = PHASYNC_G(orig_stream_select);
	}
	if (PHASYNC_G(orig_socket_select) && (f = phasync_find_ifunc("socket_select", sizeof("socket_select") - 1))) {
		f->handler = PHASYNC_G(orig_socket_select);
	}
	if (PHASYNC_G(orig_sleep) && (f = phasync_find_ifunc("sleep", sizeof("sleep") - 1))) {
		f->handler = PHASYNC_G(orig_sleep);
	}
	if (PHASYNC_G(orig_usleep) && (f = phasync_find_ifunc("usleep", sizeof("usleep") - 1))) {
		f->handler = PHASYNC_G(orig_usleep);
	}
	if (PHASYNC_G(orig_time_nanosleep) && (f = phasync_find_ifunc("time_nanosleep", sizeof("time_nanosleep") - 1))) {
		f->handler = PHASYNC_G(orig_time_nanosleep);
	}
	if (PHASYNC_G(orig_time_sleep_until) && (f = phasync_find_ifunc("time_sleep_until", sizeof("time_sleep_until") - 1))) {
		f->handler = PHASYNC_G(orig_time_sleep_until);
	}
	if (PHASYNC_G(orig_gethostbyname) && (f = phasync_find_ifunc("gethostbyname", sizeof("gethostbyname") - 1))) {
		f->handler = PHASYNC_G(orig_gethostbyname);
	}
	if (PHASYNC_G(orig_gethostbynamel) && (f = phasync_find_ifunc("gethostbynamel", sizeof("gethostbynamel") - 1))) {
		f->handler = PHASYNC_G(orig_gethostbynamel);
	}
	if (PHASYNC_G(orig_gethostbyaddr) && (f = phasync_find_ifunc("gethostbyaddr", sizeof("gethostbyaddr") - 1))) {
		f->handler = PHASYNC_G(orig_gethostbyaddr);
	}
#ifdef HAVE_FULL_DNS_FUNCS
	{
		static const char *names[] = { "dns_check_record", "checkdnsrr", "dns_get_record", "dns_get_mx", "getmxrr" };
		for (size_t k = 0; k < sizeof(names) / sizeof(names[0]); k++) {
			if ((f = phasync_find_ifunc(names[k], strlen(names[k])))) {
				if (k < 2 && PHASYNC_G(orig_dns_check_record)) f->handler = PHASYNC_G(orig_dns_check_record);
				else if (k == 2 && PHASYNC_G(orig_dns_get_record)) f->handler = PHASYNC_G(orig_dns_get_record);
				else if (k > 2 && PHASYNC_G(orig_dns_get_mx)) f->handler = PHASYNC_G(orig_dns_get_mx);
			}
		}
	}
#endif
	if (PHASYNC_G(orig_fopen) && (f = phasync_find_ifunc("fopen", sizeof("fopen") - 1))) {
		f->handler = PHASYNC_G(orig_fopen);
	}
	PHASYNC_G(hooks_installed) = 0;
}

/* ---- manage(): scoped handler activation --------------------------------- */

static void phasync_wrap_existing_streams(bool include_files);

ZEND_FUNCTION(phasync_ext_manage)
{
	zval *code, *rh, *wh, *sh;
	zend_string *timeout_name;
	zend_class_entry *timeout_ce;
	phasync_scope frame;
	zval retval;

	ZEND_PARSE_PARAMETERS_START(5, 5)
		Z_PARAM_ZVAL(code)
		Z_PARAM_ZVAL(rh)
		Z_PARAM_ZVAL(wh)
		Z_PARAM_ZVAL(sh)
		Z_PARAM_STR(timeout_name)
	ZEND_PARSE_PARAMETERS_END();

	/* Resolved once per scope; a handler exception instanceof this class (subclasses
	 * included) is the handler's way of saying "the wait's time ran out". */
	timeout_ce = zend_lookup_class(timeout_name);
	if (timeout_ce == NULL || !instanceof_function(timeout_ce, zend_ce_throwable)) {
		if (!EG(exception)) {
			zend_argument_value_error(5, "must be the name of an existing Throwable class");
		}
		RETURN_THROWS();
	}

	/* Push this scope's handlers (owned copies) and link to the enclosing one. */
	ZVAL_COPY(&frame.read,  rh);
	ZVAL_COPY(&frame.write, wh);
	ZVAL_COPY(&frame.sleep, sh);
	frame.timeout_ce = timeout_ce;
	frame.prev = PHASYNC_G(scope_top);
	PHASYNC_G(scope_top) = &frame;

	phasync_install_hooks();   /* idempotent; first manage() of the request installs */
	if (frame.prev == NULL) {
		/* Entering the outermost scope: wrap fd-backed streams that appeared after
		 * RINIT's walk — notably the STDIN/STDOUT/STDERR constants, which the CLI
		 * SAPI materialises only once execution starts, and files opened before this
		 * scope. Idempotent and cheap. */
		phasync_wrap_existing_streams(true);
	}

	ZVAL_UNDEF(&retval);
	/* zend_try/zend_catch guarantees the pop even on a fatal bailout (which
	 * longjmps past normal C control flow); a thrown PHP exception is the
	 * ordinary path — call_user_function returns and EG(exception) is set. */
	zend_try {
		call_user_function(NULL, NULL, code, &retval, 0, NULL);
	} zend_catch {
		PHASYNC_G(scope_top) = frame.prev;
		zval_ptr_dtor(&frame.read);
		zval_ptr_dtor(&frame.write);
		zval_ptr_dtor(&frame.sleep);
		zend_bailout();
	} zend_end_try();

	PHASYNC_G(scope_top) = frame.prev;
	zval_ptr_dtor(&frame.read);
	zval_ptr_dtor(&frame.write);
	zval_ptr_dtor(&frame.sleep);

	if (Z_ISUNDEF(retval)) {
		RETURN_NULL();   /* $code threw (exception pending) or returned nothing */
	}
	RETURN_COPY_VALUE(&retval);
}

/* ---- phasync\stream_select (Tier 1) -------------------------------------- */

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
			continue;
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

ZEND_FUNCTION(phasync_ext_stream_select)
{
	zval *r_array, *w_array, *e_array;
	zend_long sec, usec = 0;
	bool secnull, usecnull = 1;
	HashTable events_by_fd, revents_by_fd;
	struct pollfd *fds = NULL;
	struct timespec ts, *tsp = NULL;
	int nfds, i, retval, sets = 0, max_fd = -1, err;

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
		/* Microsecond precision like select()'s timeval (a 500 µs timeout must not
		 * become a zero-time poll), normalised like native, clamped not overflowed. */
		ts.tv_sec  = (sec > ZEND_LONG_MAX - usec / 1000000)
			? (time_t) ZEND_LONG_MAX : (time_t) (sec + usec / 1000000);
		ts.tv_nsec = (long) (usec % 1000000) * 1000;
		tsp = &ts;
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
			if ((int) fd > max_fd) {
				max_fd = (int) fd;
			}
			i++;
		} ZEND_HASH_FOREACH_END();
	}

	/* Like native stream_select(), a signal is an error (false + warning), not a
	 * silent retry that would also restart the whole timeout. */
	retval = ppoll(fds, nfds, tsp, NULL);
	err = errno;
	if (retval > 0) {
		/* select() fails with EBADF on an invalid descriptor; poll() instead flags
		 * it POLLNVAL and reports it as an event. Report it the native way. */
		for (i = 0; i < nfds; i++) {
			if (fds[i].revents & POLLNVAL) {
				retval = -1;
				err = EBADF;
				break;
			}
		}
	}
	if (retval == -1) {
		php_error_docref(NULL, E_WARNING, "Unable to select [%d]: %s (max_fd=%d)",
			err, strerror(err), max_fd);
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

	/* select() returns the number of bits set across all three sets, so a stream
	 * that is both readable and writable counts twice; poll() counts it once. */
	retval = 0;
	if (r_array) retval += phasync_filter(r_array, &revents_by_fd, POLLIN | POLLHUP | POLLERR);
	if (w_array) retval += phasync_filter(w_array, &revents_by_fd, POLLOUT | POLLERR);
	if (e_array) retval += phasync_filter(e_array, &revents_by_fd, POLLPRI);

	efree(fds);
	zend_hash_destroy(&revents_by_fd);
	zend_hash_destroy(&events_by_fd);
	RETURN_LONG(retval);
}

/* ---- module lifecycle ---------------------------------------------------- */

static void phasync_hook_entry_dtor(zval *zv)
{
	pefree(Z_PTR_P(zv), 1);
}
static void phasync_ops_dtor(zval *zv)
{
	pefree(Z_PTR_P(zv), 1);
}


PHP_INI_BEGIN()
	STD_PHP_INI_ENTRY("phasync.thread_pool_size", "8", PHP_INI_SYSTEM, OnUpdateLong,
		thread_pool_size, zend_phasync_globals, phasync_globals)
PHP_INI_END()

static PHP_MINIT_FUNCTION(phasync)
{
	REGISTER_INI_ENTRIES();
	return SUCCESS;
}

static PHP_MINFO_FUNCTION(phasync)
{
	php_info_print_table_start();
	php_info_print_table_row(2, "phasync support", "enabled");
	php_info_print_table_row(2, "version", PHP_PHASYNC_VERSION);
	php_info_print_table_end();
	DISPLAY_INI_ENTRIES();
}

static PHP_GINIT_FUNCTION(phasync)
{
#if defined(COMPILE_DL_PHASYNC) && defined(ZTS)
	ZEND_TSRMLS_CACHE_UPDATE();
#endif
	memset(phasync_globals, 0, sizeof(*phasync_globals));
	zend_hash_init(&phasync_globals->hooked, 8, NULL, phasync_hook_entry_dtor, 1);
	zend_hash_init(&phasync_globals->wrapped_ops_cache, 8, NULL, phasync_ops_dtor, 1);
}

static PHP_GSHUTDOWN_FUNCTION(phasync)
{
	zend_hash_destroy(&phasync_globals->hooked);
	zend_hash_destroy(&phasync_globals->wrapped_ops_cache);
}

static PHP_MSHUTDOWN_FUNCTION(phasync)
{
	phasync_pool_shutdown();
	UNREGISTER_INI_ENTRIES();
	return SUCCESS;
}

/* Wrap descriptor-backed streams that already exist: STDIN/STDOUT/STDERR, and
 * anything opened before the extension loaded (e.g. via ensure_loaded()'s re-exec)
 * or before the first manage() scope. Sockets, FIFOs and char devices are wrapped
 * RAW. With include_files (on entering a scope, not at RINIT), plain regular files
 * are wrapped POOL too, so a file opened before manage() is async inside it — the
 * same wrapping fopen() applies inside a scope, and the POOL ops stay native
 * outside one. This only happens for code that uses manage(): wrapping changes the
 * stream's ops identity, which php_stream_cast(AS_STDIO) checks. Streams with no OS
 * fd (php://memory/temp, data://, userspace wrappers) or with filters are left
 * alone. */
static void phasync_wrap_existing_streams(bool include_files)
{
	zend_resource *res;
	int fd_type = php_file_le_stream();

	ZEND_HASH_FOREACH_PTR(&EG(regular_list), res) {
		php_stream *stream;
		php_socket_t fd;
		struct stat st;

		if (res == NULL || res->type != fd_type) {
			continue;
		}
		stream = (php_stream *) res->ptr;
		if (stream == NULL || stream->ops == NULL) {
			continue;
		}
		if (stream->readfilters.head || stream->writefilters.head) {
			continue;   /* filters rewrite the bytes; can't raw-read the fd */
		}
		fd = phasync_stream_fd(stream);
		if (fd == -1 || fstat(fd, &st) != 0) {
			continue;
		}
		if (S_ISSOCK(st.st_mode) || S_ISFIFO(st.st_mode) || S_ISCHR(st.st_mode)) {
			phasync_wrap_stream(stream, PHASYNC_MODE_RAW);   /* idempotent */
		} else if (include_files && S_ISREG(st.st_mode) && stream->ops == &php_stream_stdio_ops) {
			phasync_wrap_stream(stream, PHASYNC_MODE_POOL);
		}
	} ZEND_HASH_FOREACH_END();
}

static PHP_RINIT_FUNCTION(phasync)
{
	PHASYNC_G(scope_top) = NULL;
	PHASYNC_G(hooks_installed) = 0;
	zend_hash_clean(&PHASYNC_G(hooked));
	/* Always-on: the transport factories and function overrides go in at the
	 * start of every request (they are inert while no manage() scope is active),
	 * and streams that predate them get wrapped now. */
	phasync_install_hooks();
	phasync_wrap_existing_streams(false);
	return SUCCESS;
}

static PHP_RSHUTDOWN_FUNCTION(phasync)
{
	phasync_restore_hooks();
	/* Any manage() scopes have unwound already (their frames live on the C
	 * stack); nothing to free here. Leave the hooked table intact: streams may
	 * close later in shutdown. */
	PHASYNC_G(scope_top) = NULL;
	return SUCCESS;
}

zend_module_entry phasync_module_entry = {
	STANDARD_MODULE_HEADER,
	"phasync",
	ext_functions,
	PHP_MINIT(phasync),
	PHP_MSHUTDOWN(phasync),
	PHP_RINIT(phasync),
	PHP_RSHUTDOWN(phasync),
	PHP_MINFO(phasync),
	PHP_PHASYNC_VERSION,
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
