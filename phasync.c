/* phasync extension
 *
 * Tier 1: phasync\stream_select() — growable, poll(2)-based, no FD_SETSIZE limit.
 *         Accepts stream resources and plain integer file descriptors.
 *
 * Tier 2: transparent async I/O. enable_hooks() re-registers the tcp:// and
 *         unix:// transports AND overrides proc_open()/sleep()/usleep(). Sockets
 *         and proc_open pipes created afterwards get their read/write ops wrapped;
 *         on a would-block the wrapper invokes the userland handler with the
 *         integer fd, which waits until ready (typically Fiber::suspend into a
 *         scheduler) and returns, then the extension performs the real I/O.
 *         sleep()/usleep() invoke a sleep handler with the duration in usec.
 *         The C side never touches the fiber API — the userland callbacks own all
 *         suspension; it works because PHP fibers are stackful.
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

#define PHP_PHASYNC_VERSION "0.3.0"

typedef struct {
	int  saved_flags;
	bool flags_saved;
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

ZEND_BEGIN_MODULE_GLOBALS(phasync)
	zval read_handler;
	zval write_handler;
	zval sleep_handler;
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
	zend_long thread_pool_size;   /* INI: phasync.thread_pool_size */
	bool hooks_enabled;
	HashTable hooked;             /* (uintptr_t)stream    -> phasync_hook_entry* */
	HashTable wrapped_ops_cache;  /* (uintptr_t)orig_ops  -> php_stream_ops*     */
ZEND_END_MODULE_GLOBALS(phasync)

ZEND_DECLARE_MODULE_GLOBALS(phasync)

#ifdef ZTS
# define PHASYNC_G(v) ZEND_MODULE_GLOBALS_ACCESSOR(phasync, v)
#else
# define PHASYNC_G(v) (phasync_globals.v)
#endif

/* ---- fd + wait helpers --------------------------------------------------- */

static php_socket_t phasync_stream_fd(php_stream *stream)
{
	php_socket_t fd = -1;
	php_stream_cast(stream, PHP_STREAM_AS_FD_FOR_SELECT | PHP_STREAM_CAST_INTERNAL,
		(void *) &fd, 0);
	return fd;
}

/* Call a userland wait handler with one long argument; -1 if it threw. */
static int phasync_call_wait(zval *handler, zend_long arg)
{
	zval args[1], retval;
	int rc = 0;

	ZVAL_LONG(&args[0], arg);
	ZVAL_UNDEF(&retval);
	if (call_user_function(NULL, NULL, handler, &retval, 1, args) == FAILURE || EG(exception)) {
		rc = -1;
	}
	zval_ptr_dtor(&retval);
	return rc;
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

	if (Z_ISUNDEF(PHASYNC_G(read_handler)) || (p = phasync_pipe_get()) == NULL) {
		/* No scheduler to yield to (or pipe failed): run inline on this thread. */
		phasync_task_exec(t);
		return;
	}

	t->write_fd = p->wfd;
	phasync_pool_ensure();
	phasync_pool_submit(t);

	/* park until the worker makes the read end readable */
	phasync_call_wait(&PHASYNC_G(read_handler), (zend_long) p->rfd);

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

	if (Z_ISUNDEF(PHASYNC_G(read_handler)) || pipe(pipefd) != 0) {
		phasync_task_exec(t);   /* no scheduler: block inline, as fopen() would */
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

	rc = phasync_call_wait(&PHASYNC_G(read_handler), (zend_long) pipefd[0]);

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

static void phasync_ensure_nonblocking(php_stream *stream, php_socket_t fd)
{
	phasync_hook_entry *e;
	int flags;

	if (fd == -1) {
		return;
	}
	e = phasync_entry(stream);
	if (e == NULL) {
		e = pemalloc(sizeof(*e), 1);
		e->saved_flags = 0;
		e->flags_saved = false;
		zend_hash_index_add_ptr(&PHASYNC_G(hooked), (zend_ulong) (uintptr_t) stream, e);
	}
	if (e->flags_saved) {
		return;
	}
	flags = fcntl(fd, F_GETFL, 0);
	if (flags != -1) {
		e->saved_flags = flags;
		e->flags_saved = true;
	}
	/* Set the STREAM non-blocking (not just the fd): openssl and the socket op
	 * consult the stream's blocking flag and would otherwise wait internally. */
	php_stream_set_option(stream, PHP_STREAM_OPTION_BLOCKING, 0, NULL);
	if (flags != -1) {
		fcntl(fd, F_SETFL, flags | O_NONBLOCK);   /* ensure O_NONBLOCK regardless */
	}
}

static ssize_t phasync_wrapped_read(php_stream *stream, char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	zval *handler = &PHASYNC_G(read_handler);

	if (fd == -1 || Z_ISUNDEF_P(handler)) {
		return orig->read(stream, buf, count);
	}
	phasync_ensure_nonblocking(stream, fd);
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
		if (phasync_call_wait(handler, (zend_long) fd) != 0) {
			return -1;
		}
	}
}

static ssize_t phasync_wrapped_write(php_stream *stream, const char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	zval *handler = &PHASYNC_G(write_handler);

	if (fd == -1 || Z_ISUNDEF_P(handler)) {
		return orig->write(stream, buf, count);
	}
	phasync_ensure_nonblocking(stream, fd);
	for (;;) {
		ssize_t n = write(fd, buf, count);
		if (n >= 0) {
			return n;
		}
		if (errno == EINTR) {
			continue;
		}
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			return -1;
		}
		if (phasync_call_wait(handler, (zend_long) fd) != 0) {
			return -1;
		}
	}
}

/* Delegate-mode ops for streams whose bytes must go through the original op
 * (e.g. TLS: SSL_read/SSL_write). We can't raw read/write the fd. We set the
 * stream non-blocking so the original op returns 0-without-eof on would-block,
 * then wait and retry. Read/write intent is approximated (a read waits for
 * readability); TLS renegotiation wanting the opposite direction is a known
 * v1 limitation. */
static ssize_t phasync_wrapped_read_tls(php_stream *stream, char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	zval *handler = &PHASYNC_G(read_handler);
	php_socket_t fd;

	if (Z_ISUNDEF_P(handler)) {
		return orig->read(stream, buf, count);
	}
	phasync_ensure_nonblocking(stream, phasync_stream_fd(stream));
	for (;;) {
		ssize_t n = orig->read(stream, buf, count);
		if (n > 0) {
			return n;
		}
		if (n < 0 || stream->eof) {
			return n;                    /* error or real EOF */
		}
		fd = phasync_stream_fd(stream);  /* n == 0, not eof -> would block */
		if (fd == -1 || phasync_call_wait(handler, (zend_long) fd) != 0) {
			return -1;
		}
	}
}

static ssize_t phasync_wrapped_write_tls(php_stream *stream, const char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	zval *handler = &PHASYNC_G(write_handler);
	php_socket_t fd;

	if (Z_ISUNDEF_P(handler)) {
		return orig->write(stream, buf, count);
	}
	phasync_ensure_nonblocking(stream, phasync_stream_fd(stream));
	for (;;) {
		ssize_t n = orig->write(stream, buf, count);
		if (n > 0) {
			return n;
		}
		if (n < 0) {
			return n;
		}
		fd = phasync_stream_fd(stream);  /* 0 -> would block */
		if (fd == -1 || phasync_call_wait(handler, (zend_long) fd) != 0) {
			return -1;
		}
	}
}

static int phasync_wrapped_close(php_stream *stream, int close_handle)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	phasync_hook_entry *e = phasync_entry(stream);

	if (e) {
		if (e->flags_saved) {
			php_socket_t fd = phasync_stream_fd(stream);
			if (fd != -1) {
				fcntl(fd, F_SETFL, e->saved_flags);
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

	if (Z_ISUNDEF(PHASYNC_G(sleep_handler))) {
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
	phasync_call_wait(&PHASYNC_G(sleep_handler), (zend_long) (seconds * 1000000));
	RETURN_LONG(0);
}

static ZEND_NAMED_FUNCTION(phasync_usleep_override)
{
	zend_long usec;

	if (Z_ISUNDEF(PHASYNC_G(sleep_handler))) {
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
	phasync_call_wait(&PHASYNC_G(sleep_handler), usec);
}

static ZEND_NAMED_FUNCTION(phasync_time_nanosleep_override)
{
	zend_long sec, nsec;

	if (Z_ISUNDEF(PHASYNC_G(sleep_handler))) {
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
	phasync_call_wait(&PHASYNC_G(sleep_handler), (zend_long) (sec * 1000000 + nsec / 1000));
	RETURN_TRUE;
}

static ZEND_NAMED_FUNCTION(phasync_time_sleep_until_override)
{
	double ts, now;
	struct timeval tv;
	zend_long usec;

	if (Z_ISUNDEF(PHASYNC_G(sleep_handler))) {
		PHASYNC_G(orig_time_sleep_until)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_DOUBLE(ts)
	ZEND_PARSE_PARAMETERS_END();

	gettimeofday(&tv, NULL);
	now = (double) tv.tv_sec + (double) tv.tv_usec / 1000000.0;
	usec = (ts > now) ? (zend_long) ((ts - now) * 1000000.0) : 0;
	phasync_call_wait(&PHASYNC_G(sleep_handler), usec);
	RETURN_TRUE;
}

static ZEND_NAMED_FUNCTION(phasync_gethostbyname_override)
{
	zend_string *host;
	phasync_task t;
	char hostbuf[256];

	if (Z_ISUNDEF(PHASYNC_G(read_handler))) {
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

	if (Z_ISUNDEF(PHASYNC_G(read_handler))) {
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

/* ---- enable_hooks / disable_hooks ---------------------------------------- */

static zend_internal_function *phasync_find_ifunc(const char *name, size_t len)
{
	zend_function *f = zend_hash_str_find_ptr(CG(function_table), name, len);
	if (f && f->type == ZEND_INTERNAL_FUNCTION) {
		return &f->internal_function;
	}
	return NULL;
}

ZEND_FUNCTION(phasync_enable_hooks)
{
	HashTable *xhash;
	zend_internal_function *f;

	ZEND_PARSE_PARAMETERS_NONE();
	if (PHASYNC_G(hooks_enabled)) {
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
	PHASYNC_G(hooks_enabled) = 1;
}

static void phasync_restore_hooks(void)
{
	zend_internal_function *f;

	if (!PHASYNC_G(hooks_enabled)) {
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
	PHASYNC_G(hooks_enabled) = 0;
}

ZEND_FUNCTION(phasync_disable_hooks)
{
	ZEND_PARSE_PARAMETERS_NONE();
	phasync_restore_hooks();
}

/* ---- handler registration ------------------------------------------------ */

static void phasync_set_handler(zval *slot, INTERNAL_FUNCTION_PARAMETERS)
{
	zend_fcall_info fci;
	zend_fcall_info_cache fcc;

	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_FUNC_OR_NULL(fci, fcc)
	ZEND_PARSE_PARAMETERS_END();

	if (!Z_ISUNDEF_P(slot)) {
		zval_ptr_dtor(slot);
		ZVAL_UNDEF(slot);
	}
	if (ZEND_FCI_INITIALIZED(fci)) {
		ZVAL_COPY(slot, &fci.function_name);
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
ZEND_FUNCTION(phasync_register_sleep_handler)
{
	phasync_set_handler(&PHASYNC_G(sleep_handler), INTERNAL_FUNCTION_PARAM_PASSTHRU);
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
	ZVAL_UNDEF(&phasync_globals->read_handler);
	ZVAL_UNDEF(&phasync_globals->write_handler);
	ZVAL_UNDEF(&phasync_globals->sleep_handler);
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

static PHP_RINIT_FUNCTION(phasync)
{
	ZVAL_UNDEF(&PHASYNC_G(read_handler));
	ZVAL_UNDEF(&PHASYNC_G(write_handler));
	ZVAL_UNDEF(&PHASYNC_G(sleep_handler));
	PHASYNC_G(hooks_enabled) = 0;
	zend_hash_clean(&PHASYNC_G(hooked));
	return SUCCESS;
}

static PHP_RSHUTDOWN_FUNCTION(phasync)
{
	phasync_restore_hooks();
	if (!Z_ISUNDEF(PHASYNC_G(read_handler)))  { zval_ptr_dtor(&PHASYNC_G(read_handler));  ZVAL_UNDEF(&PHASYNC_G(read_handler)); }
	if (!Z_ISUNDEF(PHASYNC_G(write_handler))) { zval_ptr_dtor(&PHASYNC_G(write_handler)); ZVAL_UNDEF(&PHASYNC_G(write_handler)); }
	if (!Z_ISUNDEF(PHASYNC_G(sleep_handler))) { zval_ptr_dtor(&PHASYNC_G(sleep_handler)); ZVAL_UNDEF(&PHASYNC_G(sleep_handler)); }
	/* leave hooked table intact: streams may close later in shutdown */
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
