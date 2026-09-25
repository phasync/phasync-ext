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

#define PHP_PHASYNC_VERSION "0.4.0-alpha10"

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
	void (*orig_fopen)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_stream_socket_pair)(INTERNAL_FUNCTION_PARAMETERS);
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
	char         hostresult[64];
	int          hostok;
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
			struct addrinfo hints, *res = NULL;
			memset(&hints, 0, sizeof(hints));
			hints.ai_family = AF_INET;
			hints.ai_socktype = SOCK_STREAM;
			t->hostok = 0;
			if (getaddrinfo(t->host, NULL, &hints, &res) == 0 && res) {
				struct sockaddr_in *sa = (struct sockaddr_in *) res->ai_addr;
				if (inet_ntop(AF_INET, &sa->sin_addr, t->hostresult, sizeof(t->hostresult))) {
					t->hostok = 1;
				}
			}
			if (res) freeaddrinfo(res);
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

static void phasync_pool_ensure(void)
{
	int i, n;
	pthread_attr_t attr;
	if (phasync_pool_started) {
		return;
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
	phasync_pool_started = 1;
	pthread_atfork(NULL, NULL, phasync_pool_child_atfork);
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
static void phasync_pool_run(phasync_task *t)
{
	phasync_pipe *p;
	char c;
	ssize_t r;

	if (phasync_read_handler() == NULL || EG(active_fiber) == NULL
	 || (p = phasync_pipe_get()) == NULL) {
		/* No fiber to yield from (or no scheduler / pipe failed): run inline. */
		phasync_task_exec(t);
		return;
	}

	t->write_fd = p->wfd;
	phasync_pool_ensure();
	phasync_pool_submit(t);

	/* park until the worker makes the read end readable */
	phasync_wait_fd(phasync_read_handler(), NULL, p->rfd, INFINITY);

	/* The worker will finish this bounded op shortly and write its byte even if
	 * the fiber was resumed early by an exception, so draining here is safe and
	 * leaves the pipe empty for reuse. */
	do { r = read(p->rfd, &c, 1); } while (r < 0 && errno == EINTR);
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

	rc = phasync_wait_fd(phasync_read_handler(), NULL, pipefd[0], INFINITY);

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

	if (fd == -1) {
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

	if (fd == -1) {
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

/* Record the caller's intended blocking mode and pass it through, so the native
 * op (used outside a scope) sees the real intent while we still know it. */
static int phasync_wrapped_set_option(php_stream *stream, int option, int value, void *ptrparam)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);

	if (option == PHP_STREAM_OPTION_BLOCKING) {
		phasync_hook_entry *e = phasync_entry_ensure(stream);
		e->want_block = (value != 0);
		e->applied    = -1;   /* orig is about to change the fd; re-apply on next I/O */
	}
	if (orig->set_option) {
		/* xport ops (bind/connect/listen/getname/…) all arrive through set_option,
		 * and the socket layer decides unix-vs-inet by *pointer identity*
		 * (php_stream_is(stream, &php_stream_unix_socket_ops), xp_socket.c). Our
		 * wrapped ops is a copy with a different address, which would make a unix
		 * socket look like inet ("Failed to parse address"). Restore the real ops
		 * pointer for the duration of the (synchronous) delegated call. */
		const php_stream_ops *saved = stream->ops;
		int r;
		stream->ops = (php_stream_ops *) orig;
		r = orig->set_option(stream, option, value, ptrparam);
		stream->ops = saved;
		return r;
	}
	return PHP_STREAM_OPTION_RETURN_NOTIMPL;
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
	phasync_call_sleep(phasync_sleep_handler(), (zend_long) (seconds * 1000000));
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
	phasync_call_sleep(phasync_sleep_handler(), (zend_long) (sec * 1000000 + nsec / 1000));
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
	usec = (ts > now) ? (zend_long) ((ts - now) * 1000000.0) : 0;
	phasync_call_sleep(phasync_sleep_handler(), usec);
	RETURN_TRUE;
}

static ZEND_NAMED_FUNCTION(phasync_gethostbyname_override)
{
	zend_string *host;
	phasync_task t;
	char hostbuf[256];

	if (phasync_read_handler() == NULL) {
		PHASYNC_G(orig_gethostbyname)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_STR(host)
	ZEND_PARSE_PARAMETERS_END();

	if (ZSTR_LEN(host) == 0 || ZSTR_LEN(host) >= sizeof(hostbuf)) {
		PHASYNC_G(orig_gethostbyname)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	memcpy(hostbuf, ZSTR_VAL(host), ZSTR_LEN(host));
	hostbuf[ZSTR_LEN(host)] = '\0';

	memset(&t, 0, sizeof(t));
	t.type = PHASYNC_OP_GETHOSTBYNAME;
	t.host = hostbuf;
	phasync_pool_run(&t);   /* resolves on a pool thread; parks the fiber */

	if (t.hostok) {
		RETURN_STRING(t.hostresult);
	}
	RETURN_STR_COPY(host);  /* gethostbyname() returns the input unchanged on failure */
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
	if (PHASYNC_G(orig_fopen) && (f = phasync_find_ifunc("fopen", sizeof("fopen") - 1))) {
		f->handler = PHASYNC_G(orig_fopen);
	}
	PHASYNC_G(hooks_installed) = 0;
}

/* ---- manage(): scoped handler activation --------------------------------- */

static void phasync_wrap_existing_streams(void);

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
		 * SAPI materialises only once execution starts. Idempotent and cheap. */
		phasync_wrap_existing_streams();
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

/* Wrap descriptor-backed streams that already exist when the hooks go in:
 * STDIN/STDOUT/STDERR, and anything opened before the extension loaded (e.g. via
 * ensure_loaded()'s re-exec). Only sockets, FIFOs and char devices are wrapped —
 * they can be raw-read; regular files keep their native (FILE*-buffered) ops and
 * are handled by the fopen() override, and streams with no OS fd (php://memory/
 * temp, data://, userspace wrappers) or with filters are left alone. */
static void phasync_wrap_existing_streams(void)
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
	phasync_wrap_existing_streams();
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
