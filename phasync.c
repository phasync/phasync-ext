/* phasync extension
 *
 * phasync\ext\stream_select() — growable, poll(2)-based, no FD_SETSIZE limit.
 *         Accepts stream resources and plain integer file descriptors.
 *
 * phasync\ext\Poller — waiting for streams for an event loop, on epoll: a
 *         coroutine parks in a slot of the loop (getSlot/park/unpark), and the
 *         Poller's poll() — the loop's one blocking call — unparks it.
 *
 * phasync\ext\manage($task, $poller, $sleep, $timeoutException) — run $task
 *         with transparent async I/O active for its dynamic extent.
 *         The tcp/unix/ssl transports are re-registered, and the process, sleep,
 *         DNS, file and filesystem functions, stream_select()/socket_select() and
 *         friends overridden at request start; every descriptor-backed stream is
 *         wrapped as it is created (plus STDIN/STDOUT/STDERR and any fds inherited
 *         before load). The wrappers are inert outside a scope: a wrapped stream
 *         then behaves exactly like an unwrapped one. Inside a scope, in a fiber,
 *         a would-block parks the coroutine through the scope's Poller, whose
 *         poll() unparks it when it may continue: descriptors through one-shot
 *         epoll registrations, thread-pool tasks through a queue and an eventfd. The
 *         native timeout of the call goes to park(); park() throwing
 *         $timeoutException after it ran out finishes the op the way native PHP
 *         does on a timeout, any other exception propagates. The C side never
 *         touches the fiber API — the loop owns all suspension; it works because
 *         PHP fibers are stackful. Regular files, DNS and the FIFO open()
 *         rendezvous run on worker threads (not readiness-pollable).
 */
#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_streams.h"
#include "php_network.h"
#include "SAPI.h"
#include "zend_exceptions.h"
#include "zend_closures.h"
#include "ext/spl/spl_exceptions.h"
#include <float.h>
#include "zend_attributes.h"
#include "phasync_arginfo.h"

#include <poll.h>
#include <errno.h>
#include <math.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/un.h>
#ifdef HAVE_ARPA_NAMESER_H
# include <arpa/nameser.h>
#endif
#ifdef HAVE_RESOLV_H
# include <resolv.h>
#endif
#include "ext/standard/php_dns.h"
#include "ext/standard/php_filestat.h"
#include "ext/standard/basic_functions.h"
#include "rfc1867.h"
#include "php_variables.h"
#include "php_open_temporary_file.h"
#include "zend_observer.h"
#include "zend_fibers.h"
#include "zend_extensions.h"
#if __has_include("ext/session/php_session.h")
# include "ext/session/php_session.h"
# define PHASYNC_HAVE_SESSION 1
#endif
#if __has_include("ext/filter/php_filter.h")
# include "ext/filter/php_filter.h"
# define PHASYNC_HAVE_FILTER 1
#endif
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
#include <sys/syscall.h>
#include <dirent.h>
#include <sys/wait.h>
#include <dlfcn.h>
#include <sys/file.h>
#include <sys/sysmacros.h>
#include <sys/uio.h>

#ifndef SYS_preadv2
# error "phasync needs preadv2(2) (Linux 4.6)"
#endif
#ifndef RWF_NOWAIT
# define RWF_NOWAIT 0x00000008   /* Linux 4.14 */
#endif

#ifndef SYS_pidfd_open
# define SYS_pidfd_open 434   /* Linux 5.3; same number on every architecture */
#endif

#define PHP_PHASYNC_VERSION "0.5.0-alpha19"

typedef struct {
	bool want_block;    /* caller's intended blocking mode (default: blocking) */
	signed char applied; /* fd mode we last forced: -1 unknown, 0 blocking, 1 non-blocking */
	int pidfd;          /* process pipe: pidfd of its child (the taint), or -1 */
} phasync_hook_entry;

/* Filesystem functions made cooperative (see "filesystem functions" below). */
typedef enum {
	PHASYNC_FS_WARM,        /* stat the path on the pool, then the original      */
	PHASYNC_FS_WARM_DIR,    /* read the directory on the pool, then the original */
	PHASYNC_FS_WARM_GLOB,   /* WARM_DIR on the pattern's directory               */
	PHASYNC_FS_UNLINK,      /* do it on the pool; the original only on failure   */
	PHASYNC_FS_RMDIR,
	PHASYNC_FS_MKDIR,
	PHASYNC_FS_MKDIR_P,
	PHASYNC_FS_RENAME,
	PHASYNC_FS_FSYNC,       /* fsync(t->fd): fsync() on a file stream             */
	PHASYNC_FS_FDATASYNC
} phasync_fs_op;

static const struct { const char *name; phasync_fs_op op; } phasync_fs_funcs[] = {
	{"stat", PHASYNC_FS_WARM}, {"lstat", PHASYNC_FS_WARM}, {"file_exists", PHASYNC_FS_WARM},
	{"is_file", PHASYNC_FS_WARM}, {"is_dir", PHASYNC_FS_WARM}, {"is_link", PHASYNC_FS_WARM},
	{"is_readable", PHASYNC_FS_WARM}, {"is_writable", PHASYNC_FS_WARM},
	{"is_writeable", PHASYNC_FS_WARM}, {"is_executable", PHASYNC_FS_WARM},
	{"filesize", PHASYNC_FS_WARM}, {"filemtime", PHASYNC_FS_WARM}, {"fileatime", PHASYNC_FS_WARM},
	{"filectime", PHASYNC_FS_WARM}, {"fileperms", PHASYNC_FS_WARM}, {"fileinode", PHASYNC_FS_WARM},
	{"fileowner", PHASYNC_FS_WARM}, {"filegroup", PHASYNC_FS_WARM}, {"filetype", PHASYNC_FS_WARM},
	{"linkinfo", PHASYNC_FS_WARM}, {"readlink", PHASYNC_FS_WARM}, {"realpath", PHASYNC_FS_WARM},
	{"scandir", PHASYNC_FS_WARM_DIR}, {"opendir", PHASYNC_FS_WARM_DIR}, {"dir", PHASYNC_FS_WARM_DIR},
	{"glob", PHASYNC_FS_WARM_GLOB},
	{"file_get_contents", PHASYNC_FS_WARM}, {"file_put_contents", PHASYNC_FS_WARM},
	{"file", PHASYNC_FS_WARM}, {"readfile", PHASYNC_FS_WARM}, {"copy", PHASYNC_FS_WARM},
	{"md5_file", PHASYNC_FS_WARM}, {"sha1_file", PHASYNC_FS_WARM},
	{"touch", PHASYNC_FS_WARM}, {"chmod", PHASYNC_FS_WARM}, {"chown", PHASYNC_FS_WARM},
	{"chgrp", PHASYNC_FS_WARM}, {"lchown", PHASYNC_FS_WARM}, {"lchgrp", PHASYNC_FS_WARM},
	{"link", PHASYNC_FS_WARM}, {"symlink", PHASYNC_FS_WARM}, {"tempnam", PHASYNC_FS_WARM},
	{"disk_free_space", PHASYNC_FS_WARM}, {"diskfreespace", PHASYNC_FS_WARM},
	{"disk_total_space", PHASYNC_FS_WARM},
	{"unlink", PHASYNC_FS_UNLINK}, {"rmdir", PHASYNC_FS_RMDIR}, {"mkdir", PHASYNC_FS_MKDIR},
	{"rename", PHASYNC_FS_RENAME},
};
#define PHASYNC_FS_NFUNCS (sizeof(phasync_fs_funcs) / sizeof(phasync_fs_funcs[0]))

#define PHASYNC_FS_OFFLOAD_NETWORK 0   /* phasync.fs_offload: network/FUSE mounts only */
#define PHASYNC_FS_OFFLOAD_ALL     1
#define PHASYNC_FS_OFFLOAD_NONE    2

/* How a regular file's reads run in a coroutine. */
#define PHASYNC_FS_INLINE 0   /* natively: not in a coroutine, or can't tell a cache hit */
#define PHASYNC_FS_NOWAIT 1   /* inline from the page cache, on the pool when it would block */
#define PHASYNC_FS_POOL   2   /* always on the pool */

typedef struct {
	char  *path;
	size_t len;
	dev_t  dev;
	bool   slow;                  /* network/FUSE: metadata and file data on the pool */
	bool   pool;                  /* file data on the pool: slow, or in phasync.fs_offload_types */
} phasync_mount;

/* This thread's children just before an exec-family call spawns one. */
#define PHASYNC_MAX_CHILDREN 256
typedef struct {
	pid_t before[PHASYNC_MAX_CHILDREN];
	int   n;                /* -1: /proc children unavailable */
} phasync_spawn;

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
struct phasync_poller;

typedef struct phasync_scope {
	zval poller_zv;                 /* the loop's phasync\ext\Poller (a reference)  */
	struct phasync_poller *poller;
	zval sleep;                     /* sleep(int $microseconds): void               */
	zend_class_entry *timeout_ce;   /* park() throwing this = the wait's time ran out */
	struct phasync_scope *prev;
} phasync_scope;

/* The per-request state a virtualize() boundary gets its own copy of: PHP's
 * output layer, the SAPI's response headers and request, the superglobals'
 * arrays, user-abort state and shutdown functions. Each entry is (field, the
 * live global it mirrors); the field takes the global's type, so PHP versions differing in types need no
 * #ifs, only the fields that exist differ. */
#if PHP_VERSION_ID >= 80600
# define PHASYNC_VSTATE_HEADER_CB(X) X(send_header_fcc, SG(send_header_fcc))
#else
# define PHASYNC_VSTATE_HEADER_CB(X) X(callback_func, SG(callback_func)) X(fci_cache, SG(fci_cache))
#endif
#if PHP_VERSION_ID >= 80400
# define PHASYNC_VSTATE_PARSE_BODY(X) X(parse_body, SG(request_parse_body_context))
#else
# define PHASYNC_VSTATE_PARSE_BODY(X)
#endif
#define PHASYNC_VSTATE_FIELDS(X) \
	X(ob_handlers,        OG(handlers)) \
	X(ob_active,          OG(active)) \
	X(ob_running,         OG(running)) \
	X(ob_start_file,      OG(output_start_filename)) \
	X(ob_start_line,      OG(output_start_lineno)) \
	X(ob_flags,           OG(flags)) \
	X(headers,            SG(sapi_headers)) \
	X(headers_sent,       SG(headers_sent)) \
	PHASYNC_VSTATE_HEADER_CB(X) \
	X(read_post_bytes,    SG(read_post_bytes)) \
	X(post_read,          SG(post_read)) \
	X(uploaded_files,     SG(rfc1867_uploaded_files)) \
	X(request_body,       SG(request_info).request_body) \
	X(request_method,     SG(request_info).request_method) \
	X(content_type,       SG(request_info).content_type) \
	X(content_type_dup,   SG(request_info).content_type_dup) \
	X(content_length,     SG(request_info).content_length) \
	X(headers_only,       SG(request_info).headers_only) \
	X(no_headers,         SG(request_info).no_headers) \
	X(post_entry,         SG(request_info).post_entry) \
	X(proto_num,          SG(request_info).proto_num) \
	X(query_string,       SG(request_info).query_string) \
	X(request_uri,        SG(request_info).request_uri) \
	X(cookie_data,        SG(request_info).cookie_data) \
	X(auth_user,          SG(request_info).auth_user) \
	X(auth_password,      SG(request_info).auth_password) \
	X(auth_digest,        SG(request_info).auth_digest) \
	X(request_time,       SG(global_request_time)) \
	X(http_post,          PG(http_globals)[TRACK_VARS_POST]) \
	X(http_get,           PG(http_globals)[TRACK_VARS_GET]) \
	X(http_cookie,        PG(http_globals)[TRACK_VARS_COOKIE]) \
	X(http_server,        PG(http_globals)[TRACK_VARS_SERVER]) \
	X(http_files,         PG(http_globals)[TRACK_VARS_FILES]) \
	PHASYNC_VSTATE_PARSE_BODY(X) \
	X(connection_status,  PG(connection_status)) \
	X(ignore_user_abort,  PG(ignore_user_abort)) \
	X(shutdown_functions, BG(user_shutdown_function_names)) \
	X(error_handler,      EG(user_error_handler)) \
	X(error_handler_mask, EG(user_error_handler_error_reporting)) \
	X(error_handlers,     EG(user_error_handlers)) \
	X(error_handlers_mask, EG(user_error_handlers_error_reporting)) \
	X(exception_handler,  EG(user_exception_handler)) \
	X(exception_handlers, EG(user_exception_handlers))

/* The superglobals PHP builds per request, in the order it creates them
 * (php_startup_auto_globals(), without $_ENV, which stays the process's). A
 * boundary builds its own; their symbol table entries, like $_SESSION's, are
 * global variables, which the server swaps if it wants to. */
#define PHASYNC_NSG 6
static const char *const phasync_sg_cnames[PHASYNC_NSG] = { "_GET", "_POST", "_COOKIE", "_SERVER", "_REQUEST", "_FILES" };
static zend_string *phasync_sg_names[PHASYNC_NSG];
#define PHASYNC_SG_SERVER 3

typedef struct phasync_vstate {
#define PHASYNC_VSTATE_DECL(name, live) __typeof__(live) name;
	PHASYNC_VSTATE_FIELDS(PHASYNC_VSTATE_DECL)
#undef PHASYNC_VSTATE_DECL
} phasync_vstate;

#ifdef PHASYNC_HAVE_FILTER
/* ext/filter's copies of the raw input (filter_input()), filled as the
 * superglobals are built. $_ENV's stays the process's. */
# define PHASYNC_VFILTER_FIELDS(X) X(post_array) X(get_array) X(cookie_array) X(server_array)
typedef struct phasync_vfilter {
# define PHASYNC_VFILTER_DECL(field) zval field;
	PHASYNC_VFILTER_FIELDS(PHASYNC_VFILTER_DECL)
# undef PHASYNC_VFILTER_DECL
} phasync_vfilter;
#endif

#ifdef PHASYNC_HAVE_SESSION
/* ext/session's per-request state (what its RINIT/RSHUTDOWN reset), reached
 * through PHASYNC_G(ps). Its settings behind INI entries (session.name, the cookie
 * parameters, ...) stay shared: the INI system owns those strings. */
# if PHP_VERSION_ID >= 80300
#  define PHASYNC_VSESSION_STARTED(X) X(started_file, session_started_filename) X(started_line, session_started_lineno)
# else
#  define PHASYNC_VSESSION_STARTED(X)
# endif
# if PHP_VERSION_ID >= 80600
#  define PHASYNC_VSESSION_OBJ_METHODS(X) X(user_obj_methods, mod_user_uses_object_methods_as_handlers)
# else
#  define PHASYNC_VSESSION_OBJ_METHODS(X)
# endif
# define PHASYNC_VSESSION_FIELDS(X) \
	X(id,               id) \
	X(status,           session_status) \
	X(mod,              mod) \
	X(mod_data,         mod_data) \
	X(user_names,       mod_user_names) \
	X(user_implemented, mod_user_implemented) \
	X(user_is_open,     mod_user_is_open) \
	X(user_class_name,  mod_user_class_name) \
	PHASYNC_VSESSION_OBJ_METHODS(X) \
	X(vars,             session_vars) \
	X(http_vars,        http_session_vars) \
	X(send_cookie,      send_cookie) \
	X(define_sid,       define_sid) \
	X(in_save_handler,  in_save_handler) \
	X(set_handler,      set_handler) \
	PHASYNC_VSESSION_STARTED(X)

typedef struct phasync_vsession {
# define PHASYNC_VSESSION_DECL(name, field) __typeof__(((php_ps_globals *) 0)->field) name;
	PHASYNC_VSESSION_FIELDS(PHASYNC_VSESSION_DECL)
# undef PHASYNC_VSESSION_DECL
} phasync_vsession;
#endif

struct phasync_boundary;
struct phasync_preempt;

ZEND_BEGIN_MODULE_GLOBALS(phasync)
	struct phasync_preempt *preempt; /* set_preempt_function()'s timer, NULL = none */
	zval preempt_fn;              /* its closure, UNDEF = none                     */
	zval *preempt_due;            /* phasync\ext\__PREEMPT_DUE's value: true = a checkpoint calls */
	phasync_scope *scope_top;     /* innermost active manage() scope, or NULL */
	struct phasync_boundary *vb_cur; /* virtualize(): boundary whose state is live; NULL = the SAPI's */
	phasync_vstate vroot;         /* the SAPI's own state while a boundary's is live */
	HashTable vfibers;            /* (uintptr_t)zend_fiber_context -> its boundary (members only) */
	uint32_t vcount;              /* live boundaries; 0 = the fiber observers return at once */
#ifdef PHASYNC_HAVE_SESSION
	php_ps_globals *ps;           /* ext/session's globals, NULL if it isn't loaded */
	phasync_vsession vroot_session;
#endif
#ifdef PHASYNC_HAVE_FILTER
	zend_filter_globals *fg;      /* ext/filter's globals, NULL if it isn't loaded */
	phasync_vfilter vroot_filter;
#endif
	php_stream_transport_factory orig_tcp;
	php_stream_transport_factory orig_unix;
	php_stream_transport_factory orig_udp;
	php_stream_transport_factory orig_udg;
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
	void (*orig_fclose)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_stream_socket_pair)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_stream_select)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_socket_select)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_popen)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_exec)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_system)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_passthru)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_shell_exec)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_proc_close)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_pcntl_waitpid)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_pcntl_wait)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_socket_connect)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_socket_close)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_curl_multi_select)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_sem_acquire)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_curl_exec)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_msg_receive)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_exit)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_connection_aborted)(INTERNAL_FUNCTION_PARAMETERS);
	void (*orig_connection_status)(INTERNAL_FUNCTION_PARAMETERS);
	zif_handler orig_sock[8];
	HashTable sock_hooks;         /* (uintptr_t)function_name -> index in phasync_sock_funcs */
	phasync_spawn *spawn;         /* armed by an exec-family call until its pipe is seen */
	bool ub_writing;              /* a fiber is suspended inside an echo (CLI stdout) */
	int no_suspend;               /* >0: pool ops run inline (phasync\ext\stream_select) */
	zif_handler orig_fs[PHASYNC_FS_NFUNCS];
	HashTable fs_hooks;           /* (uintptr_t)function_name -> index in phasync_fs_funcs */
	int fs_offload;               /* INI: phasync.fs_offload */
	int mountinfo_fd;             /* /proc/self/mountinfo, kept open to poll for changes */
	time_t mounts_checked;        /* monotonic second of the last poll of it */
	char *fs_offload_types;       /* INI: phasync.fs_offload_types */
	php_stream *fs_last;          /* last stdio stream whose reads were decided, and */
	int fs_last_mode;             /* how (PHASYNC_FS_*): its later reads skip deciding;
	                               * cleared when it closes */
	phasync_mount *mounts;
	int nmounts;
	bool any_slow_mount;
	bool any_pool_mount;
	dev_t *nowait_off;            /* devices whose files refuse RWF_NOWAIT (ZFS ...) */
	int nnowait_off;
	int select_depth;             /* >0 while our override probes the original select */
	zend_long thread_pool_size;   /* INI: phasync.thread_pool_size */
	bool hooks_installed;         /* transports + fn overrides physically in place */
	HashTable hooked;             /* (uintptr_t)stream    -> phasync_hook_entry* */
	HashTable inflight;           /* (uintptr_t)stream    -> phasync_inflight*   */
	HashTable ledgers;            /* (uintptr_t)zend_fiber_context -> phasync_ledger* */
	uint32_t holds;               /* closers held alive while they wait (#21)       */
	HashTable wrapped_ops_cache;  /* (uintptr_t)orig_ops  -> php_stream_ops*     */
ZEND_END_MODULE_GLOBALS(phasync)

ZEND_DECLARE_MODULE_GLOBALS(phasync)

#ifdef ZTS
# define PHASYNC_G(v) ZEND_MODULE_GLOBALS_ACCESSOR(phasync, v)
#else
# define PHASYNC_G(v) (phasync_globals.v)
#endif

/* Waits go through the innermost manage() scope. Outside one, hooked I/O falls
 * through to the original behaviour: these return 0, and the wait direction
 * (PHASYNC_READ / PHASYNC_WRITE) inside one. */
#define PHASYNC_READ   1
#define PHASYNC_WRITE  2

static zend_always_inline int phasync_reading(void)
{
	return PHASYNC_G(scope_top) ? PHASYNC_READ : 0;
}
static zend_always_inline int phasync_writing(void)
{
	return PHASYNC_G(scope_top) ? PHASYNC_WRITE : 0;
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

/* Tri-state result of a wait. */
#define PHASYNC_WAIT_READY    0    /* ready (or closed/failed) -> retry the op       */
#define PHASYNC_WAIT_TIMEOUT  1    /* native timeout ran out   -> finish like native */
#define PHASYNC_WAIT_ERROR    (-1) /* exception pending (a cancellation) -> propagate */
#define PHASYNC_WAIT_CLOSED   2    /* another coroutine closed the stream (EBADF)    */

/* PHP destroys a fiber dropped while suspended by resuming it once to unwind it:
 * its finally blocks and destructors run, and it can no longer suspend
 * (Fiber::suspend() throws). A wait there must not reach the loop at all. */
static zend_always_inline bool phasync_unwinding(void)
{
	return EG(active_fiber) && (EG(active_fiber)->flags & ZEND_FIBER_FLAG_DESTROYED);
}

/* Fail a wait in a fiber being destroyed as Fiber::suspend() would, unless the
 * unwind (or another exception) is already pending. */
static void phasync_throw_force_closed(void)
{
	if (!EG(exception)) {
		zend_throw_error(zend_hash_str_find_ptr(CG(class_table), ZEND_STRL("fibererror")),
			"Cannot suspend in a force-closed fiber");
	}
}

static bool phasync_cannot_suspend(void)
{
	if (!phasync_unwinding()) {
		return false;
	}
	phasync_throw_force_closed();
	return true;
}

/* Call the sleep handler as handler(int $microseconds). Returns -1 if it threw. */
static int phasync_call_sleep(zval *handler, zend_long usec)
{
	zval arg, retval;
	int rc = 0;

	if (phasync_cannot_suspend()) {
		return -1;
	}
	ZVAL_LONG(&arg, usec);
	ZVAL_UNDEF(&retval);
	if (call_user_function(NULL, NULL, handler, &retval, 1, &arg) == FAILURE || EG(exception)) {
		rc = -1;
	}
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

/* ---- waiting: phasync\ext\Poller -------------------------------------------
 *
 * A Poller belongs to an event loop, which gives it its getSlot(), park() and
 * unpark(). A coroutine waits by parking in a fresh slot; the Poller resumes it
 * by unparking the slot, and only ever on PHP's thread, inside its poll(). A late
 * wake-up can only find a vacant slot (unpark() returns false).
 *
 * A stream with a close hook keeps a level-triggered registration, armed for a
 * direction on its first wait and left armed after it: in request/response I/O
 * nothing arrives while the coroutine is busy, so the next wait needs no
 * syscall. An event that finds nobody waiting disarms its direction (MOD) there
 * and then, so unread data with nobody waiting costs one wake-up, never a busy
 * loop; a hang-up with nobody waiting deletes the registration (epoll reports
 * hang-ups whatever the interest). Each direction reuses one slot, unparked
 * only from its registration. Other descriptors get a one-shot registration
 * (EPOLLONESHOT), re-armed on every wait, and a fresh slot. A registration lives as
 * long as its stream, which is deregistered in its close op (epoll keeps a
 * registration while any duplicate of the descriptor lives, so close() alone is
 * not enough). A stream without a close hook keeps its registration too, until
 * another stream turns up with its descriptor number and takes it over (epoll
 * dropped it when the file closed; a stale one can only cause a spurious
 * wake-up). Bare descriptors get a registration for the one wait only.
 *
 * The thread tasks of the hooked operations under a manage() given this Poller
 * report to its completion channel: they queue the slot to wake and write to an
 * eventfd in the epoll set, never calling PHP; poll() drains the queue. The
 * channel is reference counted, so a task still running when its Poller is freed
 * finds a live channel. */

typedef struct {
	pthread_mutex_t mutex;
	zend_long      *queue;    /* slots of finished tasks, for poll()            */
	size_t          len, cap;
	int             evfd;
	int             refs;     /* the Poller + each running task; under mutex    */
} phasync_chan;

typedef struct {
	php_socket_t fd;
	php_stream  *stream;      /* owner; NULL for a bare descriptor               */
	zend_long    slot[2];     /* parked reader [0] and writer [1]; -1 = nobody   */
	zend_long    sid[2];      /* hooked: the slot each direction reuses; -1 = none yet */
	uint32_t     events;      /* hooked: the interest registered (level-triggered) */
	bool         added;       /* in the epoll set (as far as we know)            */
	bool         transient;   /* for the current wait only (a bare descriptor)   */
	bool         hooked;      /* the stream's close op deregisters it            */
} phasync_reg;

typedef struct phasync_poller {
	int           epfd;
	unsigned      fork_gen;   /* epoll and eventfd are shared with a fork()ed child */
	phasync_chan *chan;
	HashTable     regs;       /* fd -> phasync_reg*                              */
	zend_long     armed;      /* slots parked on a registration (poll(0) skips epoll without) */
	zval          get_slot, park, unpark;
	struct phasync_poller *next;   /* the live Pollers (for the close hooks)     */
	zend_object   std;
} phasync_poller;

#define PHASYNC_EP_COMPLETIONS UINT64_MAX

static zend_class_entry *phasync_poller_ce;
static zend_object_handlers phasync_poller_handlers;
static phasync_poller *phasync_pollers;   /* live Pollers of this process */

/* Bumped in every fork()ed child (pthread_atfork), so a Poller can tell it is
 * used from a process other than its creator without a getpid() syscall. */
static unsigned phasync_fork_gen;

static void phasync_preempt_fork_prepare(void);
static void phasync_preempt_fork_parent(void);
static void phasync_preempt_fork_child(void);

static void phasync_fork_child(void)
{
	phasync_fork_gen++;
	phasync_preempt_fork_child();
}

static zend_always_inline phasync_poller *phasync_poller_from(zend_object *obj)
{
	return (phasync_poller *) ((char *) obj - XtOffsetOf(phasync_poller, std));
}

static void phasync_reg_dtor(zval *zv)
{
	pefree(Z_PTR_P(zv), 1);
}

static void phasync_chan_release(phasync_chan *ch)
{
	int refs;

	pthread_mutex_lock(&ch->mutex);
	refs = --ch->refs;
	pthread_mutex_unlock(&ch->mutex);
	if (refs == 0) {
		close(ch->evfd);
		free(ch->queue);
		pthread_mutex_destroy(&ch->mutex);
		free(ch);
	}
}

/* Queue a slot for poll() to unpark, from any thread. */
static void phasync_chan_push(phasync_chan *ch, zend_long slot)
{
	uint64_t one = 1;
	ssize_t w;

	pthread_mutex_lock(&ch->mutex);
	if (ch->len == ch->cap) {
		size_t cap = ch->cap ? ch->cap * 2 : 64;
		zend_long *q = realloc(ch->queue, cap * sizeof(*q));
		if (q) {
			ch->queue = q;
			ch->cap = cap;
		}
	}
	if (ch->len < ch->cap) {
		ch->queue[ch->len++] = slot;
	}
	pthread_mutex_unlock(&ch->mutex);
	do { w = write(ch->evfd, &one, sizeof(one)); } while (w < 0 && errno == EINTR);
}

/* Take back a slot's queued wake-up: its coroutine stopped waiting (cancelled,
 * timed out, or destroyed), and poll() must not unpark it. */
static void phasync_chan_purge(phasync_chan *ch, zend_long slot)
{
	size_t j = 0;

	pthread_mutex_lock(&ch->mutex);
	for (size_t i = 0; i < ch->len; i++) {
		if (ch->queue[i] != slot) {
			ch->queue[j++] = ch->queue[i];
		}
	}
	ch->len = j;
	pthread_mutex_unlock(&ch->mutex);
}

/* ---- the ledger: what the extension holds for a suspended coroutine --------
 *
 * While a coroutine is suspended inside a hooked operation, the extension holds
 * things for it: a registration and slot in a Poller, a count in the stream's
 * in-flight record (#14), a pool operation, a closer's wait. Each is entered in
 * the coroutine's ledger before it suspends and struck off by the code after the
 * suspension. When PHP destroys a suspended fiber it resumes it once to unwind it,
 * and that code settles everything; whatever it never gets back to (a fatal error
 * unwinding past it) the fiber's destroy observer settles, and at the end of the
 * request whatever is left. None of it depends on PHP code (the application's
 * finally blocks, the loop's bookkeeping): a slot is taken back from the Poller
 * before its coroutine is gone, so the extension never wakes a dead one. */
struct phasync_task;
typedef struct {
	phasync_poller *wait_p;       /* parked on a registration of it (a reference): */
	php_stream     *wait_stream;
	php_socket_t    wait_fd;
	int             wait_idx, wait_dupfd;
	zend_long       wait_slot;
	php_stream     *inflight;     /* inside an op on it: one of its waiters (#14)  */
	php_stream     *closing;      /* its closer, parked in closing_p (a reference) */
	phasync_poller *closing_p;
	struct phasync_task *task;    /* a pool operation reporting to task_p (a reference) */
	phasync_poller *task_p;
} phasync_ledger;

static phasync_ledger *phasync_ledger_cur(void)
{
	zend_ulong key = (zend_ulong) (uintptr_t) EG(current_fiber_context);
	phasync_ledger *l = zend_hash_index_find_ptr(&PHASYNC_G(ledgers), key);

	if (l == NULL) {
		l = pecalloc(1, sizeof(*l), 1);
		zend_hash_index_add_new_ptr(&PHASYNC_G(ledgers), key, l);
	}
	return l;
}

/* Throw unless this process created the Poller: in a fork()ed child its epoll
 * and eventfd instances are the parent's. */
static bool phasync_poller_usable(phasync_poller *p)
{
	if (p->fork_gen == phasync_fork_gen) {
		return true;
	}
	zend_throw_error(NULL, "This Poller belongs to another process; create a new one after fork()");
	return false;
}

/* A new slot from the loop's getSlot(); -1 if it threw. */
static zend_long phasync_get_slot(phasync_poller *p)
{
	zval retval;
	zend_long slot = -1;

	ZVAL_UNDEF(&retval);
	if (call_user_function(NULL, NULL, &p->get_slot, &retval, 0, NULL) == SUCCESS && !EG(exception)) {
		slot = zval_get_long(&retval);
	}
	zval_ptr_dtor(&retval);
	return slot;
}

/* Park the current coroutine in $slot. READY when unparked. With a timeout
 * class (the manage() scope's, for a hooked call with a native timeout), park()
 * throwing it once the timeout given has run out clears the exception: TIMEOUT,
 * finish the op the way PHP does on a timeout. Otherwise (the public
 * readable()/writable(), or any other exception, a cancellation) it stays
 * pending: ERROR. */
static int phasync_park(phasync_poller *p, zend_long slot, double timeout, zend_class_entry *timeout_ce)
{
	zval args[2], retval;
	double deadline = isinf(timeout) ? INFINITY : phasync_now() + timeout;
	int rc = PHASYNC_WAIT_READY;

	if (phasync_cannot_suspend()) {
		return PHASYNC_WAIT_ERROR;
	}
	ZVAL_LONG(&args[0], slot);
	ZVAL_DOUBLE(&args[1], isinf(timeout) ? DBL_MAX : timeout);
	ZVAL_UNDEF(&retval);
	if (call_user_function(NULL, NULL, &p->park, &retval, 2, args) == FAILURE) {
		rc = PHASYNC_WAIT_ERROR;
	} else if (EG(exception)) {
		if (timeout_ce && !isinf(deadline) && instanceof_function(EG(exception)->ce, timeout_ce)
		 && phasync_now() + 0.001 >= deadline) {
			zend_clear_exception();
			rc = PHASYNC_WAIT_TIMEOUT;
		} else {
			rc = PHASYNC_WAIT_ERROR;
		}
	}
	zval_ptr_dtor(&retval);
	if (rc != PHASYNC_WAIT_READY) {
		phasync_chan_purge(p->chan, slot);
	}
	return rc;
}

/* Arm the registration for whoever is parked on it (one-shot); with nobody,
 * leave it disarmed. Returns -1 (errno set) if epoll refused the descriptor. */
static int phasync_reg_ctl(phasync_poller *p, phasync_reg *r, uint32_t ev, bool oneshot)
{
	struct epoll_event e;

	e.events = ev | (oneshot ? EPOLLONESHOT : 0);
	e.data.u64 = (uint64_t) r->fd;
	if (epoll_ctl(p->epfd, r->added ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, r->fd, &e) != 0) {
		/* A taken-over registration: epoll may have dropped it (the file closed), or
		 * the descriptor may be another file it has never seen. */
		if (!(errno == ENOENT && r->added) && !(errno == EEXIST && !r->added)) {
			return -1;
		}
		if (epoll_ctl(p->epfd, r->added ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, r->fd, &e) != 0) {
			return -1;
		}
	}
	r->added = true;
	r->events = ev;
	return 0;
}

/* Make the registration cover whoever is parked on it: a hooked one only ever
 * gains interest here (a no-op costs no syscall), a one-shot one is re-armed. */
static int phasync_reg_arm(phasync_poller *p, phasync_reg *r)
{
	uint32_t want = (r->slot[0] >= 0 ? EPOLLIN | EPOLLRDHUP : 0) | (r->slot[1] >= 0 ? EPOLLOUT : 0);

	if (r->hooked) {
		return r->added && (r->events | want) == r->events ? 0 : phasync_reg_ctl(p, r, r->events | want, false);
	}
	return phasync_reg_ctl(p, r, want, true);
}

static void phasync_reg_drop(phasync_poller *p, phasync_reg *r)
{
	if (r->added) {
		epoll_ctl(p->epfd, EPOLL_CTL_DEL, r->fd, NULL);
	}
	zend_hash_index_del(&p->regs, (zend_ulong) r->fd);   /* frees r */
}

/* A stream is closing: deregister it (from every live Poller) before its
 * descriptor goes away, and wake whoever is parked on it (through poll(), like
 * any wake-up); their next use of the stream finds it closed. */
static void phasync_stream_forget(php_stream *stream)
{
	php_socket_t fd = -2;

	if (stream == PHASYNC_G(fs_last)) {
		PHASYNC_G(fs_last) = NULL;
	}

	for (phasync_poller *p = phasync_pollers; p; p = p->next) {
		phasync_reg *r;

		if (zend_hash_num_elements(&p->regs) == 0) {
			continue;
		}
		if (fd == -2) {
			fd = phasync_stream_fd(stream);
		}
		if (fd == -1 || p->fork_gen != phasync_fork_gen
		 || (r = zend_hash_index_find_ptr(&p->regs, (zend_ulong) fd)) == NULL || r->stream != stream) {
			continue;
		}
		for (int i = 0; i < 2; i++) {
			if (r->slot[i] >= 0) {
				phasync_chan_push(p->chan, r->slot[i]);
				p->armed--;
			}
		}
		phasync_reg_drop(p, r);
	}
}

static bool phasync_has_close_hook(php_stream *stream);

/* A wait on a registration is over (or its coroutine gone): if nobody unparked
 * it, disarm it, and let go of the Poller. The stream may have been closed
 * meanwhile (its registration freed): look it up again. */
static void phasync_wait_settle(phasync_poller *p, int idx, php_stream *stream, php_socket_t fd,
                                zend_long slot, int dupfd)
{
	phasync_reg *r = zend_hash_index_find_ptr(&p->regs, (zend_ulong) fd);

	if (r && r->stream != stream) {
		r = NULL;
	}
	if (r && r->slot[idx] == slot) {
		r->slot[idx] = -1;
		p->armed--;
		phasync_reg_arm(p, r);
	}
	if (r && r->transient && r->slot[0] < 0 && r->slot[1] < 0) {
		phasync_reg_drop(p, r);
	}
	if (dupfd >= 0) {
		close(dupfd);
	}
	OBJ_RELEASE(&p->std);
}

/* Wait (in a fiber) until fd is readable or writable, or closed or failed.
 * timeout is the native timeout, INFINITY for none. Returns READY, TIMEOUT or
 * ERROR (exception pending); see phasync_park(). A regular file is always
 * ready: with via_loop (Poller::readable()/writable()) the coroutine still
 * waits for the loop's next poll(), as phasync's own wait on it does without
 * the extension (#29); hooked operations go on at once. */
static int phasync_poller_wait(phasync_poller *p, int dir, php_stream *stream, php_socket_t fd,
                               double timeout, zend_class_entry *timeout_ce, bool via_loop)
{
	int idx = dir == PHASYNC_WRITE, rc, dupfd = -1;
	phasync_reg *r;
	phasync_ledger *l;
	zend_long slot;

	if (!phasync_poller_usable(p)) {
		return PHASYNC_WAIT_ERROR;
	}
	if (fd == -1) {
		zend_throw_error(NULL, "The stream has no file descriptor to wait on");
		return PHASYNC_WAIT_ERROR;
	}
	r = zend_hash_index_find_ptr(&p->regs, (zend_ulong) fd);
	if (r && stream && r->stream && r->stream != stream) {
		if (r->hooked) {
			/* The other stream's close op never deregistered it: a bug in the
			 * extension. Never re-register quietly. */
			zend_error_noreturn(E_ERROR, "phasync: descriptor %d is still registered for stream %p, now waited on by stream %p",
				(int) fd, (void *) r->stream, (void *) stream);
		}
		/* The previous owner had no close hook: it is gone, its descriptor reused. */
		r->stream = stream;
		r->hooked = phasync_has_close_hook(stream);
		r->sid[0] = r->sid[1] = -1;
	}
	if (r && !stream) {
		/* A bare-descriptor wait on a descriptor a stream owns (echo to STDOUT):
		 * wait on a duplicate, which epoll keeps apart. */
		if ((dupfd = dup(fd)) < 0) {
			zend_throw_error(NULL, "Unable to wait on descriptor %d: %s", (int) fd, strerror(errno));
			return PHASYNC_WAIT_ERROR;
		}
		fd = dupfd;
		r = NULL;
	}
	if (r == NULL) {
		r = pecalloc(1, sizeof(*r), 1);
		r->fd = fd;
		r->stream = stream;
		r->slot[0] = r->slot[1] = -1;
		r->sid[0] = r->sid[1] = -1;
		r->transient = stream == NULL;
		r->hooked = stream && phasync_has_close_hook(stream);
		zend_hash_index_add_new_ptr(&p->regs, (zend_ulong) fd, r);
	}
	if (r->slot[idx] >= 0) {
		zend_throw_exception_ex(spl_ce_LogicException, 0, "Another coroutine is already waiting to %s this stream",
			idx ? "write to" : "read from");
		rc = PHASYNC_WAIT_ERROR;
		goto done;
	}
	if ((slot = r->hooked && r->sid[idx] >= 0 ? r->sid[idx] : phasync_get_slot(p)) < 0) {
		rc = PHASYNC_WAIT_ERROR;
		goto done;
	}
	if (r->hooked) {
		r->sid[idx] = slot;
	}
	r->slot[idx] = slot;
	p->armed++;
	if (phasync_reg_arm(p, r) != 0) {
		r->slot[idx] = -1;
		p->armed--;
		if (errno == EPERM && via_loop) {
			phasync_chan_push(p->chan, slot);   /* a regular file: ready at the next poll() */
			goto park;
		} else if (errno == EPERM) {
			rc = PHASYNC_WAIT_READY;   /* a regular file: always ready, as select() says */
		} else {
			zend_throw_error(NULL, "Unable to wait on descriptor %d: %s", (int) fd, strerror(errno));
			rc = PHASYNC_WAIT_ERROR;
		}
		goto done;
	}

park:
	GC_ADDREF(&p->std);                  /* the loop may drop the Poller while we wait */
	l = phasync_ledger_cur();
	l->wait_p = p;
	l->wait_stream = stream;
	l->wait_fd = fd;
	l->wait_idx = idx;
	l->wait_dupfd = dupfd;
	l->wait_slot = slot;
	rc = phasync_park(p, slot, timeout, timeout_ce);
	l->wait_p = NULL;
	phasync_wait_settle(p, idx, stream, fd, slot, dupfd);
	return rc;
done:
	if (r && r->transient && r->slot[0] < 0 && r->slot[1] < 0) {
		phasync_reg_drop(p, r);
	}
	if (dupfd >= 0) {
		close(dupfd);
	}
	return rc;
}

/* ---- a stream closed under a suspended coroutine (#14) --------------------
 *
 * A coroutine suspended inside an operation on a stream still has the stream on
 * its C stack: in our op, in PHP's stream layer, in mysqlnd (with the connection
 * mysqli frees as it closes). Another coroutine closing the stream then must not
 * free it before they have left: its close op wakes them, parks the closer until
 * the last one is out, and only then lets the close go on. A woken operation fails
 * as on a closed descriptor (EBADF), and so does any later one that would wait on
 * the closing stream. PHP frees a stream as its close op returns, whatever the
 * path (fclose(), mysqli::close(), ...), so the close op is where it waits.
 *
 * The closer's fiber is held alive while it waits: destroyed there, it could
 * neither wait nor keep the stream from being freed under the others (#21). Held,
 * it is resumed by its loop once they are out, or, if its loop has let it go, left
 * to PHP to destroy once they are (phasync_holds_release()). */
typedef struct {
	uint32_t        waiters;      /* coroutines suspended in an op on the stream */
	bool            closing;      /* a close woke them; ops on it fail (EBADF)   */
	phasync_poller *closer;       /* where the closer is parked (NULL: none), and its slot */
	zend_long       closer_slot;
	zend_object    *hold;         /* the closer's fiber, held while it waits     */
} phasync_inflight;

/* Before suspending in an op on the stream. False (errno EBADF) if it is closing. */
static bool phasync_inflight_enter(php_stream *stream)
{
	phasync_inflight *f = zend_hash_index_find_ptr(&PHASYNC_G(inflight), (zend_ulong) (uintptr_t) stream);

	if (f == NULL) {
		f = pecalloc(1, sizeof(*f), 1);
		zend_hash_index_add_new_ptr(&PHASYNC_G(inflight), (zend_ulong) (uintptr_t) stream, f);
	} else if (f->closing) {
		errno = EBADF;
		return false;
	}
	f->waiters++;
	phasync_ledger_cur()->inflight = stream;
	return true;
}

/* One coroutine less inside an op on the stream; the last one out of a closing
 * stream lets the closer go on. True if it is closing. */
static bool phasync_inflight_out(php_stream *stream)
{
	phasync_inflight *f = zend_hash_index_find_ptr(&PHASYNC_G(inflight), (zend_ulong) (uintptr_t) stream);

	if (--f->waiters == 0) {
		if (!f->closing) {
			zend_hash_index_del(&PHASYNC_G(inflight), (zend_ulong) (uintptr_t) stream);
			return false;
		}
		if (f->closer) {
			phasync_chan_push(f->closer->chan, f->closer_slot);
		}
	}
	return f->closing;
}

/* Back from the suspension. True (errno EBADF) if the stream is closing: the op fails. */
static bool phasync_inflight_leave(php_stream *stream)
{
	phasync_ledger_cur()->inflight = NULL;
	if (phasync_inflight_out(stream)) {
		errno = EBADF;
		return true;
	}
	return false;
}

/* Let go of a closer's fiber once it no longer waits. Resumed by its loop, the
 * loop holds it too; if only we do, it is running and must not be freed under
 * itself: then the reference is left to the end of the request. */
static void phasync_hold_drop(phasync_inflight *f)
{
	zend_object *o = f->hold;

	f->hold = NULL;
	PHASYNC_G(holds)--;
	if (GC_REFCOUNT(o) > 1) {
		GC_DELREF(o);
	}
}

/* Where no coroutine is inside an op (a Poller's poll(), a manage() starting): let
 * PHP have the closers held for streams nobody is inside any more. One whose loop
 * let it go is destroyed here, and its close ends as it unwinds. */
static void phasync_holds_release(void)
{
	zend_object *release[16];
	phasync_inflight *f;
	int n = 0;

	if (PHASYNC_G(holds) == 0) {
		return;
	}
	ZEND_HASH_FOREACH_PTR(&PHASYNC_G(inflight), f) {
		if (f->hold && f->waiters == 0 && n < 16) {
			release[n++] = f->hold;
			f->hold = NULL;
			PHASYNC_G(holds)--;
		}
	} ZEND_HASH_FOREACH_END();
	for (int i = 0; i < n; i++) {
		OBJ_RELEASE(release[i]);
	}
}

/* The request is ending and PHP destroys every object: a closer destroyed while
 * coroutines are still inside an op on its stream cannot wait for them, but they
 * are about to be destroyed too. Destroy them first, as PHP would. */
static void phasync_inflight_destroy_waiters(php_stream *stream)
{
	zend_object *victims[16];
	phasync_ledger *l;
	zend_ulong key;
	int n = 0;

	ZEND_HASH_FOREACH_NUM_KEY_PTR(&PHASYNC_G(ledgers), key, l) {
		zend_fiber_context *ctx = (zend_fiber_context *) (uintptr_t) key;
		if (l->inflight == stream && ctx->kind == zend_ce_fiber && n < 16) {
			zend_object *obj = &((zend_fiber *) ((char *) ctx - XtOffsetOf(zend_fiber, context)))->std;
			if (!(OBJ_FLAGS(obj) & IS_OBJ_DESTRUCTOR_CALLED)) {
				GC_ADDREF(obj);
				victims[n++] = obj;
			}
		}
	} ZEND_HASH_FOREACH_END();
	for (int i = 0; i < n; i++) {
		GC_ADD_FLAGS(victims[i], IS_OBJ_DESTRUCTOR_CALLED);
		victims[i]->handlers->dtor_obj(victims[i]);
		OBJ_RELEASE(victims[i]);
	}
}

/* Wake whoever waits on the closing stream and park until every coroutine
 * suspended in an op on it has left. A cancellation of the wait is held until
 * they are out. False if the closer is being destroyed while they are still in
 * (it began closing while unwinding): it cannot wait, and the unwind is pending. */
static bool phasync_inflight_drain(php_stream *stream, phasync_inflight *f)
{
	phasync_poller *p = PHASYNC_G(scope_top)->poller;
	phasync_ledger *l = phasync_ledger_cur();
	zend_object *held = NULL;
	bool destroyed_waiters = false;

	phasync_stream_forget(stream);
	f->closing = true;
	f->closer = p;
	GC_ADDREF(&p->std);
	l->closing = stream;
	l->closing_p = p;
	if (!phasync_unwinding()) {
		f->hold = &EG(active_fiber)->std;
		GC_ADDREF(f->hold);
		PHASYNC_G(holds)++;
	}
	while (f->waiters) {
		if (phasync_unwinding()) {
			if ((EG(flags) & EG_FLAGS_IN_SHUTDOWN) && !destroyed_waiters) {
				destroyed_waiters = true;
				phasync_inflight_destroy_waiters(stream);
				continue;
			}
			break;                   /* the unwind stays pending */
		}
		if ((f->closer_slot = phasync_get_slot(p)) < 0
		 || phasync_park(p, f->closer_slot, INFINITY, NULL) != PHASYNC_WAIT_READY) {
			if (EG(exception) == NULL) {
				zend_error_noreturn(E_ERROR, "phasync: unable to wait for the coroutines using a stream being closed");
			}
			if (phasync_unwinding()) {
				continue;
			}
			if (held == NULL) {
				held = EG(exception);
				GC_ADDREF(held);
			}
			zend_clear_exception();
		}
	}
	l->closing = NULL;
	f->closer = NULL;
	OBJ_RELEASE(&p->std);
	if (f->hold) {
		phasync_hold_drop(f);
	}
	if (f->waiters) {
		if (held) {
			OBJ_RELEASE(held);
		}
		return false;
	}
	if (held) {
		zend_throw_exception_internal(held);
	}
	return true;
}

/* From a close op, before the stream goes: wait until no coroutine is inside an op
 * on it (see phasync_inflight_drain()). The closer can only wait in a coroutine;
 * anywhere else it would free the stream under them, so that is a fatal error. */
static void phasync_inflight_close(php_stream *stream)
{
	phasync_inflight *f = zend_hash_index_find_ptr(&PHASYNC_G(inflight), (zend_ulong) (uintptr_t) stream);

	phasync_stream_forget(stream);
	if (f == NULL) {
		return;
	}
	if (f->waiters) {
		if (PHASYNC_G(scope_top) == NULL || EG(active_fiber) == NULL) {
			zend_error_noreturn(E_ERROR, "phasync: a stream was closed outside a coroutine while %u coroutine(s) wait on it",
				f->waiters);
		}
		if (!phasync_inflight_drain(stream, f)) {
			zend_error_noreturn(E_ERROR, "phasync: a stream was closed by a coroutine being destroyed while %u coroutine(s) wait on it",
				f->waiters);
		}
	}
	zend_hash_index_del(&PHASYNC_G(inflight), (zend_ulong) (uintptr_t) stream);
}

/* The hooked operations wait through the manage() scope's Poller, with its
 * timeout class. A wait on a stream fails with CLOSED if another coroutine
 * closes it meanwhile. */
static int phasync_wait_fd(int dir, php_stream *stream, php_socket_t fd, double timeout)
{
	phasync_scope *s = PHASYNC_G(scope_top);
	int rc;

	if (stream == NULL) {
		return phasync_poller_wait(s->poller, dir, stream, fd, timeout, s->timeout_ce, false);
	}
	if (!phasync_inflight_enter(stream)) {
		return PHASYNC_WAIT_CLOSED;
	}
	rc = phasync_poller_wait(s->poller, dir, stream, fd, timeout, s->timeout_ce, false);
	return phasync_inflight_leave(stream) && rc != PHASYNC_WAIT_ERROR ? PHASYNC_WAIT_CLOSED : rc;
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
static int phasync_cooperate(int dir, php_stream *stream, php_socket_t fd, double *deadline)
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
	w = phasync_wait_fd(dir, stream, fd, wait);
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
	PHASYNC_OP_FS,             /* filesystem metadata/namespace op (phasync_fs_op) */
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
	int          omode;       /* OPEN: open(2) mode; FS: mkdir(2) mode          */
	phasync_fs_op fsop;       /* FS: which operation                           */
	char         path2[PATH_MAX]; /* FS rename: destination                     */
	zend_long    slot;        /* the waiter's slot, queued for poll() when done  */
	void        *chan;        /* its Poller's completion channel (a reference)   */
	int          done;        /* set (under the channel's mutex) once finished   */
	int          orphaned;    /* its coroutine is gone: the thread frees it (ditto) */
	int          ownfd;       /* its own duplicate of fd (READ/WRITE/fsync), or -1 */
	bool         dedicated;   /* on its own thread: */
	pthread_t    thread;
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

/* Worker side of PHASYNC_OP_FS: plain syscalls on t->path (absolute). */
static int phasync_fs_exec(phasync_task *t)
{
	struct stat st;
	DIR *d;
	char *p;

	switch (t->fsop) {
		case PHASYNC_FS_WARM:
			lstat(t->path, &st);
			return stat(t->path, &st);
		case PHASYNC_FS_WARM_DIR:
		case PHASYNC_FS_WARM_GLOB:
			if ((d = opendir(t->path)) == NULL) {
				return -1;
			}
			while (readdir(d) != NULL);
			return closedir(d);
		case PHASYNC_FS_UNLINK:
			return unlink(t->path);
		case PHASYNC_FS_RMDIR:
			return rmdir(t->path);
		case PHASYNC_FS_RENAME:
			return rename(t->path, t->path2);
		case PHASYNC_FS_MKDIR_P:
			for (p = t->path; (p = strchr(p + 1, '/')) != NULL; *p = '/') {
				*p = '\0';
				if (mkdir(t->path, t->omode) < 0 && errno != EEXIST) {
					*p = '/';
					return -1;
				}
			}
			ZEND_FALLTHROUGH;
		case PHASYNC_FS_MKDIR:
			return mkdir(t->path, t->omode);
		case PHASYNC_FS_FSYNC:
			return fsync(t->fd);
		case PHASYNC_FS_FDATASYNC:
			return fdatasync(t->fd);
	}
	return -1;
}

static void phasync_task_exec(phasync_task *t)
{
	switch (t->type) {
		case PHASYNC_OP_FS:
			t->result = phasync_fs_exec(t);
			t->err = errno;
			break;
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

/* ---- a thread's task: owned by the extension, not by the coroutine ----------
 *
 * A task handed to a thread is a heap copy with its own buffer, its own copy of
 * the host name and its own duplicate of the descriptor: the thread never points
 * into the coroutine's frame or into PHP's data, and the descriptor can't be
 * closed (and its number reused) under it. The waiter collects the results once
 * the thread is done. If the waiter stops waiting first (cancelled, destroyed, or
 * its fiber gone without coming back), the task is orphaned: the thread frees it
 * when it finishes, and nobody waits for that. */
static phasync_task *phasync_task_own(phasync_task *t)
{
	phasync_task *h = malloc(sizeof(*h));

	if (h == NULL) {
		return NULL;
	}
	*h = *t;
	h->orphaned = 0;
	h->dedicated = false;
	h->ownfd = -1;
	h->host = NULL;
	h->buf = NULL;
	h->ai = NULL;
	if (t->type == PHASYNC_OP_OPEN) {
		h->fd = -1;
	}
	bool host = t->type == PHASYNC_OP_GETHOSTBYNAME || t->type == PHASYNC_OP_GETADDRINFO
	         || t->type == PHASYNC_OP_DNSQUERY;
	bool buf = t->type == PHASYNC_OP_READ || t->type == PHASYNC_OP_WRITE || t->type == PHASYNC_OP_DNSQUERY;
	if ((host && (h->host = strdup(t->host)) == NULL)
	 || (buf && (h->buf = malloc(t->count ? t->count : 1)) == NULL)) {
		goto fail;
	}
	if (t->type == PHASYNC_OP_WRITE) {
		memcpy(h->buf, t->buf, t->count);
	}
	if (t->type == PHASYNC_OP_READ || t->type == PHASYNC_OP_WRITE
	 || (t->type == PHASYNC_OP_FS && (t->fsop == PHASYNC_FS_FSYNC || t->fsop == PHASYNC_FS_FDATASYNC))) {
		if ((h->ownfd = h->fd = fcntl(t->fd, F_DUPFD_CLOEXEC, 0)) < 0) {
			goto fail;
		}
	}
	return h;
fail:
	free(h->buf);
	free((char *) h->host);
	free(h);
	return NULL;
}

/* From any thread: free a task and whatever it still owns. */
static void phasync_task_free(phasync_task *h)
{
	free(h->buf);
	free((char *) h->host);
	if (h->ownfd >= 0) {
		close(h->ownfd);
	}
	if (h->type == PHASYNC_OP_OPEN && h->fd >= 0) {
		close(h->fd);
	}
	if (h->ai) {
		freeaddrinfo(h->ai);
	}
	free(h);
}

/* A task is done: mark it done and queue its slot for poll(); an orphan the thread
 * frees instead. The thread never touches the task after this: its waiter may
 * free it at once. */
static void phasync_task_signal(phasync_task *t)
{
	zend_long slot = t->slot;
	phasync_chan *ch = t->chan;
	int orphaned;

	pthread_mutex_lock(&ch->mutex);
	t->done = 1;
	orphaned = t->orphaned;
	pthread_mutex_unlock(&ch->mutex);
	if (orphaned) {
		phasync_task_free(t);
	} else {
		phasync_chan_push(ch, slot);
	}
	phasync_chan_release(ch);            /* the task's reference */
}

/* The waiter is done with its task (and lets go of the Poller): with the thread
 * done, copy the results into out (NULL: drop them) and free it; otherwise orphan
 * it (a dedicated thread is cancelled: it may block forever) and fail out with
 * ECANCELED. Never waits. */
static bool phasync_task_finish(phasync_task *h, phasync_poller *p, phasync_task *out)
{
	phasync_chan *ch = h->chan;
	zend_long slot = h->slot;
	pthread_t th = h->thread;
	bool dedicated = h->dedicated;
	int done;

	pthread_mutex_lock(&ch->mutex);
	done = h->done;
	if (!done) {
		h->orphaned = 1;                 /* the thread frees it from here on */
	}
	pthread_mutex_unlock(&ch->mutex);
	if (dedicated) {
		if (!done) {
			pthread_cancel(th);
		}
		pthread_detach(th);
	}
	phasync_chan_purge(p->chan, slot);
	OBJ_RELEASE(&p->std);
	if (!done) {
		if (out) {
			out->result = -1;
			out->err = ECANCELED;
			out->hostok = 0;
			if (out->type == PHASYNC_OP_OPEN) {
				out->fd = -1;
			}
		}
		return false;
	}
	if (out) {
		if ((h->type == PHASYNC_OP_READ || h->type == PHASYNC_OP_DNSQUERY) && h->result > 0) {
			memcpy(out->buf, h->buf, MIN((size_t) h->result, out->count));
		}
		out->result = h->result;
		out->err = h->err;
		out->hostok = h->hostok;
		out->naddrs = h->naddrs;
		memcpy(out->addrs, h->addrs, sizeof(h->addrs));
		memcpy(out->hostresult, h->hostresult, sizeof(h->hostresult));
		out->ai = h->ai;
		h->ai = NULL;
		if (h->type == PHASYNC_OP_OPEN) {
			out->fd = h->fd;
			h->fd = -1;
		}
	}
	phasync_task_free(h);
	return true;
}

/* Give a task its Poller's channel (a reference, released by the thread). */
static phasync_chan *phasync_task_chan(phasync_task *t, phasync_poller *p)
{
	phasync_chan *ch = p->chan;

	pthread_mutex_lock(&ch->mutex);
	ch->refs++;
	pthread_mutex_unlock(&ch->mutex);
	t->chan = ch;
	t->done = 0;
	return ch;
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

/* Cancelled inside open(): the task was orphaned (that is when it is cancelled). */
static void phasync_oneshot_cancelled(void *arg)
{
	phasync_task *t = (phasync_task *) arg;
	phasync_chan *ch = t->chan;

	phasync_task_free(t);                /* closes the descriptor if open() had returned */
	phasync_chan_release(ch);
}

/* Dedicated one-shot thread for a single indefinitely-blocking task (FIFO
 * open). It runs the task, signals, and exits (detached). Cancellation is
 * allowed only across the blocking open() (a POSIX cancellation point): its
 * orphaned task is freed by the cleanup handler. */
static void *phasync_oneshot(void *arg)
{
	phasync_task *t = (phasync_task *) arg;

	pthread_cleanup_push(phasync_oneshot_cancelled, t);
	phasync_thread_block_signals();
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	phasync_task_exec(t);
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
	pthread_cleanup_pop(0);

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

/* Submit a task and park the coroutine until a worker has done it. Without a
 * coroutine to park (a fiber being destroyed cannot), a worker, or a slot, run it
 * inline: a pool task always ends. On a cancellation the task is left to its
 * thread and fails with ECANCELED (the exception pending). */
static void phasync_pool_run(phasync_task *t)
{
	phasync_poller *p = PHASYNC_G(scope_top) ? PHASYNC_G(scope_top)->poller : NULL;
	phasync_ledger *l;
	phasync_task *h;

	if (!p || EG(active_fiber) == NULL || PHASYNC_G(no_suspend) || phasync_unwinding()
	 || p->fork_gen != phasync_fork_gen || !phasync_pool_ensure() || (t->slot = phasync_get_slot(p)) < 0
	 || (h = phasync_task_own(t)) == NULL) {
		phasync_task_exec(t);
		return;
	}
	phasync_task_chan(h, p);
park:
	GC_ADDREF(&p->std);                  /* the loop may drop the Poller while we wait */
	l = phasync_ledger_cur();
	l->task = h;
	l->task_p = p;
	phasync_pool_submit(h);
	phasync_park(p, h->slot, INFINITY, NULL);
	l->task = NULL;
	phasync_task_finish(h, p, t);
}

/* Like phasync_pool_run, but on a dedicated thread rather than the bounded pool.
 * Used for FIFO open(), whose rendezvous can block indefinitely — a bounded
 * pool would deadlock when reader- and writer-opens outnumber the workers. */
static void phasync_pool_run_dedicated(phasync_task *t)
{
	phasync_poller *p = PHASYNC_G(scope_top) ? PHASYNC_G(scope_top)->poller : NULL;
	phasync_ledger *l;
	phasync_task *h;
	pthread_attr_t attr;
	int rc;

	if (!p || EG(active_fiber) == NULL || phasync_unwinding() || p->fork_gen != phasync_fork_gen
	 || (t->slot = phasync_get_slot(p)) < 0 || (h = phasync_task_own(t)) == NULL) {
		phasync_task_exec(t);   /* no coroutine to park: block inline, as fopen() would */
		return;
	}
	h->dedicated = true;
	phasync_task_chan(h, p);
	pthread_attr_init(&attr);
	pthread_attr_setstacksize(&attr, PHASYNC_WORKER_STACK);
	rc = pthread_create(&h->thread, &attr, phasync_oneshot, h);
	pthread_attr_destroy(&attr);
	if (rc != 0) {
		phasync_chan_release(h->chan);
		phasync_task_free(h);
		phasync_task_exec(t);
		return;
	}
	GC_ADDREF(&p->std);
	l = phasync_ledger_cur();
	l->task = h;
	l->task_p = p;
	phasync_park(p, h->slot, INFINITY, NULL);
	l->task = NULL;
	/* The worker left t->fd as a valid fd, or -1 with t->err set (a real open()
	 * failure, or ECANCELED); the caller distinguishes the two. */
	phasync_task_finish(h, p, t);
}

/* Pool-mode ops: offload the blocking read/write to a worker thread (for regular
 * files, which are not readiness-pollable). Falls back to the original op when
 * there's no scheduler or no fd. */
static ssize_t phasync_pool_read_fd(php_stream *stream, php_socket_t fd, char *buf, size_t count)
{
	phasync_task t;

	memset(&t, 0, sizeof(t));
	t.type = PHASYNC_OP_READ;
	t.fd = fd;
	t.buf = buf;
	t.count = count;
	if (!phasync_inflight_enter(stream)) {
		return -1;
	}
	phasync_pool_run(&t);   /* a close meanwhile waits for it: the read is done */
	phasync_inflight_leave(stream);
	errno = t.err;
	if (t.result == 0) {
		stream->eof = 1;   /* a 0-byte read on a regular file is EOF (as plain
		                    * stdio read does); without this feof() never trips
		                    * and while (!feof($fp)) spins. */
	}
	php_clear_stat_cache(0, NULL, 0);   /* as the native read: atime changed */
	return t.result;
}

static ssize_t phasync_pool_write_fd(php_stream *stream, php_socket_t fd, const char *buf, size_t count)
{
	phasync_task t;

	memset(&t, 0, sizeof(t));
	t.type = PHASYNC_OP_WRITE;
	t.fd = fd;
	t.buf = (char *) buf;
	t.count = count;
	if (!phasync_inflight_enter(stream)) {
		return -1;
	}
	phasync_pool_run(&t);
	phasync_inflight_leave(stream);
	errno = t.err;
	return t.result;
}

static ssize_t phasync_wrapped_read_pool(php_stream *stream, char *buf, size_t count)
{
	php_socket_t fd = phasync_stream_fd(stream);

	/* Outside a scope (or not in a fiber) there is nothing to yield to: be the
	 * native op exactly. */
	if (fd == -1 || !phasync_reading() || EG(active_fiber) == NULL) {
		return PHASYNC_ORIG(stream)->read(stream, buf, count);
	}
	return phasync_pool_read_fd(stream, fd, buf, count);
}

static ssize_t phasync_wrapped_write_pool(php_stream *stream, const char *buf, size_t count)
{
	php_socket_t fd = phasync_stream_fd(stream);

	if (fd == -1 || !phasync_reading() || EG(active_fiber) == NULL) {
		return PHASYNC_ORIG(stream)->write(stream, buf, count);
	}
	return phasync_pool_write_fd(stream, fd, buf, count);
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
		e->pidfd       = -1;
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
 *   - outside a manage() scope                  -> exactly the native op;
 *   - an explicitly non-blocking stream          -> exactly the native op;
 *   - a blocking stream inside a scope           -> cooperate: drive the fd
 *     non-blocking and suspend the fiber on would-block instead of blocking.
 * So a wrapped stream is indistinguishable from an unwrapped one whenever no
 * scope is driving it. */
static ssize_t phasync_wrapped_read(php_stream *stream, char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	int dir = phasync_reading();
	phasync_hook_entry *e;

	if (fd == -1) {
		return orig->read(stream, buf, count);
	}
	e = phasync_entry_ensure(stream);
	if (!e->want_block || !dir) {
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
		if (phasync_cooperate(dir, stream, fd, &deadline) != PHASYNC_COOP_RETRY) {
			return -1;                   /* timed out (flag set) or exception pending */
		}
	}
}

static ssize_t phasync_wrapped_write(php_stream *stream, const char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	int dir = phasync_writing();
	phasync_hook_entry *e;

	if (fd == -1) {
		return orig->write(stream, buf, count);
	}
	e = phasync_entry_ensure(stream);
	if (!e->want_block || !dir) {
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
		c = phasync_cooperate(dir, stream, fd, &deadline);
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
	int dir = phasync_reading();
	phasync_hook_entry *e = phasync_entry_ensure(stream);

	if (!e->want_block || !dir) {
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
		if (fd == -1 || phasync_cooperate(dir, stream, fd, &deadline) != PHASYNC_COOP_RETRY) {
			return -1;
		}
	}
}

static ssize_t phasync_wrapped_write_tls(php_stream *stream, const char *buf, size_t count)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	int dir = phasync_writing();
	phasync_hook_entry *e = phasync_entry_ensure(stream);

	if (!e->want_block || !dir) {
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
		if (fd == -1 || phasync_cooperate(dir, stream, fd, &deadline) != PHASYNC_COOP_RETRY) {
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
	if (saved->read == phasync_wrapped_read_tls) {
		/* ext/openssl never compares ops pointers, and its connect/accept enable
		 * crypto through stream->ops: keep ours there, so the handshake cooperates
		 * and accepted clients inherit the wrapped ops. */
		return orig->set_option(stream, option, value, ptrparam);
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
			? phasync_wait_fd(PHASYNC_READ, stream, fd, remaining)
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
		if (w == PHASYNC_WAIT_ERROR || w == PHASYNC_WAIT_CLOSED) {
			/* Closed meanwhile (EBADF), or the wait cancelled or its coroutine
			 * destroyed (ECANCELED, the exception pending propagates). */
			xp->outputs.client = NULL;
			xp->outputs.returncode = -1;
			xp->outputs.error_code = w == PHASYNC_WAIT_CLOSED ? EBADF : ECANCELED;
			if (xp->want_errortext) {
				xp->outputs.error_text = php_socket_error_str(xp->outputs.error_code);
			}
			return PHP_STREAM_OPTION_RETURN_OK;
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
				? phasync_wait_fd(PHASYNC_WRITE, NULL, sock->socket, remaining)
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
/* stream_socket_recvfrom()/sendto() go through the transport (recvfrom()/
 * sendto() on the socket), not the read/write ops. Inside a scope, in a fiber,
 * on a blocking stream: wait until the socket is ready, then let the original
 * do the call in blocking mode, which then doesn't wait, so results and
 * warnings stay native. PHP applies no timeout here, so neither does the wait.
 * Out-of-band data goes straight to the original. */
static int phasync_xport_io_cooperative(php_stream *stream, php_stream_xport_param *xp, int option, int value)
{
	phasync_hook_entry *e = phasync_entry_ensure(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	int dir = xp->op == STREAM_XPORT_OP_RECV ? PHASYNC_READ : PHASYNC_WRITE;

	if (e->want_block && fd != -1 && !(xp->inputs.flags & STREAM_OOB)) {
		struct pollfd pfd = { fd, dir == PHASYNC_READ ? POLLIN : POLLOUT, 0 };
		while (poll(&pfd, 1, 0) == 0) {
			if (phasync_wait_fd(dir, stream, fd, INFINITY) != PHASYNC_WAIT_READY) {
				return PHP_STREAM_OPTION_RETURN_ERR;   /* exception pending */
			}
		}
		phasync_apply_mode(stream, fd, e, false, false);
	}
	return phasync_orig_set_option(stream, option, value, xp);
}

static php_stream_ops *phasync_wrapped_ops_for(const php_stream_ops *orig, phasync_mode mode);

/* Enabling crypto (stream_socket_enable_crypto(), and the handshake in tls://
 * connect and accept) on a blocking stream loops on the handshake, polling the
 * socket in between. Inside a scope, in a fiber: run it non-blocking, where each
 * call makes one attempt and returns 0 while it would block, and park between
 * attempts, as native PHP polls (no timeout, as natively). Which way to wait is
 * not exposed; a handshake that wants to write finds the socket not writable, so
 * wait for writable then, else readable. Afterwards the stream's reads and writes
 * must go through the original (SSL_read/SSL_write), or come off it again. */
static int phasync_crypto_enable(php_stream *stream, int option, int value, php_stream_xport_crypto_param *cp)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	phasync_hook_entry *e = phasync_entry_ensure(stream);
	php_socket_t fd = phasync_stream_fd(stream);
	int rc;

	if (cp->inputs.activate && e->want_block && fd != -1 && phasync_reading() && EG(active_fiber) != NULL) {
		orig->set_option(stream, PHP_STREAM_OPTION_BLOCKING, 0, NULL);
		for (;;) {
			struct pollfd pfd = { fd, POLLOUT, 0 };
			rc = orig->set_option(stream, option, value, cp);
			if (rc != PHP_STREAM_OPTION_RETURN_OK || cp->outputs.returncode != 0) {
				break;
			}
			if (phasync_wait_fd(poll(&pfd, 1, 0) == 0 ? PHASYNC_WRITE : PHASYNC_READ, stream, fd, INFINITY)
			    != PHASYNC_WAIT_READY) {
				cp->outputs.returncode = -1;     /* exception pending */
				break;
			}
		}
		orig->set_option(stream, PHP_STREAM_OPTION_BLOCKING, 1, NULL);
		e->applied = 0;
	} else {
		rc = orig->set_option(stream, option, value, cp);
	}
	if (rc == PHP_STREAM_OPTION_RETURN_OK && cp->outputs.returncode == 1 && cp->inputs.activate
	 && stream->ops->read == phasync_wrapped_read) {
		stream->ops = phasync_wrapped_ops_for(orig, PHASYNC_MODE_TLS);
	} else if (!cp->inputs.activate && stream->ops->read == phasync_wrapped_read_tls) {
		/* crypto is off after a disable (which returns -1 even when it succeeds) */
		stream->ops = phasync_wrapped_ops_for(orig, PHASYNC_MODE_RAW);
	}
	return rc;
}

static void phasync_wrap_stream(php_stream *stream, phasync_mode mode);
static int phasync_wrapped_set_option_inner(php_stream *stream, int option, int value, void *ptrparam);

/* An accepted client is created with the listener's ops at the time, which are
 * the original ones while the socket layer runs (phasync_orig_set_option()):
 * give it the listener's wrapper, so it cooperates and its close op deregisters it. */
static int phasync_wrapped_set_option(php_stream *stream, int option, int value, void *ptrparam)
{
	int rc = phasync_wrapped_set_option_inner(stream, option, value, ptrparam);

	if (option == PHP_STREAM_OPTION_XPORT_API && ptrparam
	 && ((php_stream_xport_param *) ptrparam)->op == STREAM_XPORT_OP_ACCEPT
	 && ((php_stream_xport_param *) ptrparam)->outputs.client) {
		phasync_wrap_stream(((php_stream_xport_param *) ptrparam)->outputs.client,
			stream->ops->read == phasync_wrapped_read_tls ? PHASYNC_MODE_TLS : PHASYNC_MODE_RAW);
	}
	return rc;
}

static int phasync_wrapped_set_option_inner(php_stream *stream, int option, int value, void *ptrparam)
{
	if (option == PHP_STREAM_OPTION_BLOCKING) {
		phasync_hook_entry *e = phasync_entry_ensure(stream);
		e->want_block = (value != 0);
		e->applied    = -1;   /* orig is about to change the fd; re-apply on next I/O */
	} else if (option == PHP_STREAM_OPTION_CRYPTO_API && ptrparam
	        && ((php_stream_xport_crypto_param *) ptrparam)->op == STREAM_XPORT_CRYPTO_OP_ENABLE) {
		return phasync_crypto_enable(stream, option, value, (php_stream_xport_crypto_param *) ptrparam);
	} else if (option == PHP_STREAM_OPTION_MMAP_API && value == PHP_STREAM_MMAP_SUPPORTED
	        && stream->ops->read == phasync_wrapped_read_pool
	        && phasync_reading() && EG(active_fiber) != NULL) {
		/* mmap()ed reads (readfile(), fpassthru(), stream_copy_to_stream()) fault
		 * pages in synchronously: report mmap unsupported so they use the pooled
		 * read op instead. */
		return PHP_STREAM_OPTION_RETURN_NOTIMPL;
	} else if (option == PHP_STREAM_OPTION_XPORT_API && ptrparam
	        && ((php_stream_xport_param *) ptrparam)->op == STREAM_XPORT_OP_ACCEPT
	        && phasync_reading() && EG(active_fiber) != NULL) {
		return phasync_accept_cooperative(stream, (php_stream_xport_param *) ptrparam);
	} else if (option == PHP_STREAM_OPTION_XPORT_API && ptrparam
	        && (((php_stream_xport_param *) ptrparam)->op == STREAM_XPORT_OP_RECV
	         || ((php_stream_xport_param *) ptrparam)->op == STREAM_XPORT_OP_SEND)
	        && stream->ops->read == phasync_wrapped_read
	        && phasync_reading() && EG(active_fiber) != NULL) {
		return phasync_xport_io_cooperative(stream, (php_stream_xport_param *) ptrparam, option, value);
	} else if (option == PHP_STREAM_OPTION_XPORT_API && ptrparam
	        && ((php_stream_xport_param *) ptrparam)->op == STREAM_XPORT_OP_CONNECT
	        && stream->ops->read == phasync_wrapped_read       /* not tls://: crypto follows */
	        && phasync_writing() && EG(active_fiber) != NULL
	        && (strcmp(PHASYNC_ORIG(stream)->label, "tcp_socket") == 0
	            || strcmp(PHASYNC_ORIG(stream)->label, "tcp_socket/ssl") == 0)) {
		return phasync_connect_cooperative(stream, (php_stream_xport_param *) ptrparam);
	}
	return phasync_orig_set_option(stream, option, value, ptrparam);
}

static int phasync_wrapped_close(php_stream *stream, int close_handle)
{
	const php_stream_ops *orig = PHASYNC_ORIG(stream);
	phasync_hook_entry *e;

	phasync_inflight_close(stream);
	e = phasync_entry(stream);

	if (e) {
		if (e->pidfd != -1) {
			/* A process pipe: pclose() below waits for the child. Inside a scope,
			 * in a fiber, wait for its exit here instead, on the pidfd (readable
			 * once the child exits, without reaping it), so pclose() then reaps at
			 * once with the native status. Like pclose(), first close our end, by
			 * duping /dev/null over it (the fd stays valid for fclose()), so a
			 * child still using the pipe gets EOF/EPIPE instead of blocking. */
			int dir = phasync_reading();
			php_socket_t fd = phasync_stream_fd(stream);
			int devnull;
			if (close_handle && dir && EG(active_fiber) && fd != -1
			 && (devnull = open("/dev/null", O_RDWR | O_CLOEXEC)) >= 0) {
				if (dup2(devnull, fd) >= 0) {
					phasync_wait_fd(dir, NULL, e->pidfd, INFINITY);
				}
				close(devnull);
			}
			close(e->pidfd);
		}
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

static php_stream *phasync_udp_factory(const char *proto, size_t protolen,
		const char *resourcename, size_t resourcenamelen, const char *persistent_id,
		int options, int flags, struct timeval *timeout,
		php_stream_context *context STREAMS_DC)
{
	php_stream *s = PHASYNC_G(orig_udp)(proto, protolen, resourcename, resourcenamelen,
		persistent_id, options, flags, timeout, context STREAMS_CC);
	phasync_wrap_stream(s, PHASYNC_MODE_RAW);
	return s;
}

static php_stream *phasync_udg_factory(const char *proto, size_t protolen,
		const char *resourcename, size_t resourcenamelen, const char *persistent_id,
		int options, int flags, struct timeval *timeout,
		php_stream_context *context STREAMS_DC)
{
	php_stream *s = PHASYNC_G(orig_udg)(proto, protolen, resourcename, resourcenamelen,
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

static int phasync_fs_path(zval *z, char *out);
static bool phasync_fs_fd_offloaded(php_socket_t fd);
static int phasync_fs_fd_mode(php_socket_t fd);

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

	if (!phasync_reading() || EG(active_fiber) == NULL) {
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

	if (!phasync_reading() || EG(active_fiber) == NULL) {
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

	if (!phasync_reading() || EG(active_fiber) == NULL) {
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

	if (!phasync_reading() || EG(active_fiber) == NULL) {
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

	if (!phasync_reading() || EG(active_fiber) == NULL) {
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

	if (!phasync_reading() || EG(active_fiber) == NULL) {
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

	if (!phasync_reading()) {
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

	/* On a network/FUSE mount even the stat() below can stall: warm it on the pool. */
	if (phasync_reading() && EG(active_fiber)) {
		phasync_task w;
		zval z;
		ZVAL_STR(&z, filename);
		if (phasync_fs_path(&z, w.path) == 1) {
			w.type = PHASYNC_OP_FS;
			w.fsop = PHASYNC_FS_WARM;
			phasync_pool_run(&w);
			if (EG(exception)) {
				RETURN_THROWS();
			}
		}
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

	/* Not a FIFO: open normally, then POOL-wrap regular files that are offloaded. */
	PHASYNC_G(orig_fopen)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
	if (Z_TYPE_P(return_value) == IS_RESOURCE) {
		php_stream *s = NULL;
		php_stream_from_zval_no_verify(s, return_value);
		if (s) {
			php_socket_t fd = phasync_stream_fd(s);
			if (fd != -1 && phasync_fs_fd_offloaded(fd)) {
				phasync_wrap_stream(s, PHASYNC_MODE_POOL);
			}
		}
	}
}

/* ---- process family: popen(), exec(), system(), passthru(), shell_exec(),
 * proc_close() --------------------------------------------------------------
 *
 * Their pipe reads/writes cooperate through the wrapped ops like any pipe; the
 * blocking wait for the child in pclose()/proc_close() cooperates through a pidfd
 * of the child. popen() doesn't expose the pid, so it is found by diffing this
 * thread's /proc children around the spawn (an unreaped child's pid can't be
 * reused, so that is race-free). Without /proc children or pidfd_open() (Linux
 * < 5.3) the child wait stays native. */

/* This thread's children from /proc; -1 when unavailable. */
static int phasync_children(pid_t *out)
{
	char path[64], buf[PHASYNC_MAX_CHILDREN * 8], *p, *end;
	ssize_t len;
	int fd, n = 0;

	snprintf(path, sizeof(path), "/proc/self/task/%ld/children", (long) syscall(SYS_gettid));
	if ((fd = open(path, O_RDONLY | O_CLOEXEC)) < 0) {
		return -1;
	}
	len = read(fd, buf, sizeof(buf) - 1);
	close(fd);
	if (len < 0 || len == (ssize_t) sizeof(buf) - 1) {
		return -1;                       /* unreadable, or too many to be sure */
	}
	buf[len] = '\0';
	for (p = buf; n < PHASYNC_MAX_CHILDREN; p = end) {
		long pid = strtol(p, &end, 10);
		if (end == p) {
			break;
		}
		out[n++] = (pid_t) pid;
	}
	return n;
}

/* A pidfd of the one child spawned since sp was taken; -1 if not exactly one. */
static int phasync_new_child_pidfd(const phasync_spawn *sp)
{
	pid_t now[PHASYNC_MAX_CHILDREN], pid = 0;
	int n, i, j;

	if (sp->n < 0 || (n = phasync_children(now)) < 0) {
		return -1;
	}
	for (i = 0; i < n; i++) {
		for (j = 0; j < sp->n && sp->before[j] != now[i]; j++);
		if (j < sp->n) {
			continue;
		}
		if (pid) {
			return -1;
		}
		pid = now[i];
	}
	return pid ? (int) syscall(SYS_pidfd_open, pid, 0) : -1;
}

static ssize_t (*phasync_stdio_read_orig)(php_stream *stream, char *buf, size_t count);
static int (*phasync_stdio_close_orig)(php_stream *stream, int close_handle);

/* Streams whose close op deregisters them (phasync_stream_forget()): stdio
 * streams and every wrapped stream. */
static bool phasync_has_close_hook(php_stream *stream)
{
	return stream->ops == &php_stream_stdio_ops
		|| stream->ops->read == phasync_wrapped_read
		|| stream->ops->read == phasync_wrapped_read_tls
		|| stream->ops->read == phasync_wrapped_read_pool;
}

static void phasync_inflight_close(php_stream *stream);

static int phasync_stdio_close(php_stream *stream, int close_handle)
{
	phasync_inflight_close(stream);
	return phasync_stdio_close_orig(stream, close_handle);
}
static ssize_t (*phasync_stdio_write_orig)(php_stream *stream, const char *buf, size_t count);
static int (*phasync_stdio_set_option_orig)(php_stream *stream, int option, int value, void *ptrparam);

/* The leading fields of plain_wrapper.c's php_stdio_stream_data, the same from
 * PHP 8.2 to master. */
typedef struct {
	FILE *file;
	int   fd;
} phasync_stdio_head;

/* php_stream_stdio_ops.read/write see every plain file stream: fopen()'d ones on
 * a local disk, and those opened internally (file_get_contents(),
 * file_put_contents(), file(), copy(), readfile(), md5_file() ...), which bypass
 * the fopen() override. Inside a scope, in a fiber, their reads run as
 * phasync_fs_fd_mode() says (and mmap is declined unless inline), and writes go
 * to the pool for a pool file. Returns PHASYNC_FS_INLINE for a stream that stays
 * native, and sets *fdp otherwise.
 * The stream keeps the stdio ops: PHP checks php_stream_is(STDIO) on streams it
 * builds internally (php://temp's spill file, for one) and breaks if they change.
 * A stream with a C FILE* (its own buffer) stays native. Never for a file being
 * compiled: include/require (under a user frame) and the internal functions that
 * compile (opcache_compile_file() ...) read through a stdio stream too, but must
 * not suspend mid-compile. Zend's streams are the unbuffered ones marked
 * auto-cleanup (__exposed); file_get_contents() also unbuffers its stream, but
 * never exposes it. */
static int phasync_stdio_mode(php_stream *stream, php_socket_t *fdp)
{
	zend_execute_data *ex = EG(current_execute_data);
	phasync_stdio_head *h = (phasync_stdio_head *) stream->abstract;

	if (!phasync_reading() || EG(active_fiber) == NULL
	 || stream->ops != &php_stream_stdio_ops || h->file != NULL || h->fd < 0
	 || ((stream->flags & PHP_STREAM_FLAG_NO_BUFFER) && stream->__exposed)
	 || ex == NULL || ex->func == NULL || ZEND_USER_CODE(ex->func->type)
	 || stream->readfilters.head || stream->writefilters.head) {
		return PHASYNC_FS_INLINE;
	}
	*fdp = h->fd;
	if (stream != PHASYNC_G(fs_last)) {
		PHASYNC_G(fs_last) = stream;         /* a file stays on its filesystem */
		PHASYNC_G(fs_last_mode) = phasync_fs_fd_mode(h->fd);
	}
	return PHASYNC_G(fs_last_mode);
}

static int phasync_stdio_set_option(php_stream *stream, int option, int value, void *ptrparam)
{
	/* fsync()/fdatasync() wait for the disk: inside a scope, in a fiber, on the
	 * pool. A stream with a C FILE* (its own buffer, to flush first on PHP's
	 * thread) stays native; plain file streams write straight to the fd. */
	if (option == PHP_STREAM_OPTION_SYNC_API
	 && (value == PHP_STREAM_SYNC_FSYNC || value == PHP_STREAM_SYNC_FDSYNC)
	 && phasync_reading() && EG(active_fiber) != NULL
	 && ((phasync_stdio_head *) stream->abstract)->file == NULL
	 && ((phasync_stdio_head *) stream->abstract)->fd >= 0) {
		phasync_task t;
		memset(&t, 0, sizeof(t));
		t.type = PHASYNC_OP_FS;
		t.fsop = value == PHP_STREAM_SYNC_FSYNC ? PHASYNC_FS_FSYNC : PHASYNC_FS_FDATASYNC;
		t.fd = ((phasync_stdio_head *) stream->abstract)->fd;
		if (!phasync_inflight_enter(stream)) {
			return PHP_STREAM_OPTION_RETURN_ERR;
		}
		phasync_pool_run(&t);
		phasync_inflight_leave(stream);
		if (EG(exception)) {
			return PHP_STREAM_OPTION_RETURN_ERR;
		}
		errno = t.err;
		return t.result == 0 ? PHP_STREAM_OPTION_RETURN_OK : PHP_STREAM_OPTION_RETURN_ERR;
	}
	/* flock() (and LOCK_EX in file_put_contents(), SplFileObject::flock()) blocks
	 * in flock(2) until the lock is free, with no descriptor to wait on. Inside a
	 * scope, in a fiber, try without blocking and sleep through the sleep handler
	 * between tries (1ms, doubling to 20ms). Plain and wrapped stdio streams both
	 * end up here. */
	if (option == PHP_STREAM_OPTION_LOCKING && ptrparam == NULL
	 && (value == LOCK_SH || value == LOCK_EX)
	 && phasync_sleep_handler() != NULL && EG(active_fiber) != NULL) {
		zend_long usec = 1000;
		int rc;
		while ((rc = phasync_stdio_set_option_orig(stream, option, value | LOCK_NB, NULL)) != 0
		       && errno == EWOULDBLOCK) {
			if (!phasync_inflight_enter(stream)) {
				return -1;
			}
			rc = phasync_call_sleep(phasync_sleep_handler(), usec);
			if (phasync_inflight_leave(stream) || rc < 0) {
				return -1;               /* closed meanwhile, or exception pending */
			}
			usec = MIN(usec * 2, 20000);
		}
		return rc;
	}
	php_socket_t fd;
	if (option == PHP_STREAM_OPTION_MMAP_API && value == PHP_STREAM_MMAP_SUPPORTED
	 && phasync_stdio_mode(stream, &fd) != PHASYNC_FS_INLINE) {
		return PHP_STREAM_OPTION_RETURN_NOTIMPL;   /* a page fault could block: read */
	}
	return phasync_stdio_set_option_orig(stream, option, value, ptrparam);
}

static ssize_t phasync_stdio_write(php_stream *stream, const char *buf, size_t count)
{
	php_socket_t fd;

	/* A buffered write waits for the disk only under writeback pressure, and
	 * RWF_NOWAIT would refuse any write that allocates blocks: only a pool file
	 * writes on the pool. */
	if (phasync_stdio_mode(stream, &fd) == PHASYNC_FS_POOL) {
		return phasync_pool_write_fd(stream, fd, buf, count);
	}
	return phasync_stdio_write_orig(stream, buf, count);
}

/* php_stream_stdio_ops.read/write, patched process-wide at MINIT, for streams
 * built internally, where there is no other hook: regular files (above), and the
 * pipe of php_exec()/shell_exec(). For those an exec-family override arms
 * PHASYNC_G(spawn); they read their popen() pipe right after spawning, before
 * anything else runs, so the first FIFO read then is that pipe: taint it with the
 * child's pidfd and wrap it. */
static ssize_t phasync_stdio_read(php_stream *stream, char *buf, size_t count)
{
	phasync_spawn *sp = PHASYNC_G(spawn);
	php_socket_t fd;
	struct stat st;

	if (sp && stream->ops == &php_stream_stdio_ops
	 && (fd = phasync_stream_fd(stream)) != -1 && fstat(fd, &st) == 0 && S_ISFIFO(st.st_mode)) {
		PHASYNC_G(spawn) = NULL;
		phasync_entry_ensure(stream)->pidfd = phasync_new_child_pidfd(sp);
		phasync_wrap_stream(stream, PHASYNC_MODE_RAW);
		return stream->ops->read(stream, buf, count);
	}
	switch (phasync_stdio_mode(stream, &fd)) {
		case PHASYNC_FS_POOL:
			return phasync_pool_read_fd(stream, fd, buf, count);
		case PHASYNC_FS_NOWAIT: {
			/* From the page cache only, at the current position (-1) as read()
			 * does. A short read returns as is: PHP reads a plain file greedily,
			 * so the next read asks for the rest, and at EOF finds 0 as it would
			 * natively. */
			struct iovec iov = { buf, count };
			ssize_t n = syscall(SYS_preadv2, fd, &iov, 1, -1L, -1L, RWF_NOWAIT);
			if (n >= 0) {
				if (n == 0) {
					stream->eof = 1;
				}
				php_clear_stat_cache(0, NULL, 0);   /* as the native read: atime changed */
				return n;
			}
			if (errno == EAGAIN) {
				/* Not cached: it would wait for the disk. Only a regular file
				 * reads on the pool (a pipe says EAGAIN when empty). */
				if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode)) {
					return phasync_pool_read_fd(stream, fd, buf, count);
				}
			} else if (errno == EOPNOTSUPP || errno == EINVAL || errno == ENOSYS) {
				/* This filesystem (or kernel) can't tell: its files read inline,
				 * as natively, and none of them tries again. */
				if (fstat(fd, &st) == 0) {
					dev_t *nd = realloc(PHASYNC_G(nowait_off), (PHASYNC_G(nnowait_off) + 1) * sizeof(dev_t));
					if (nd) {
						PHASYNC_G(nowait_off) = nd;
						nd[PHASYNC_G(nnowait_off)++] = st.st_dev;
					}
				}
				PHASYNC_G(fs_last_mode) = PHASYNC_FS_INLINE;
			}
			break;
		}
	}
	return phasync_stdio_read_orig(stream, buf, count);
}

static void phasync_exec_family(INTERNAL_FUNCTION_PARAMETERS, void (*orig)(INTERNAL_FUNCTION_PARAMETERS))
{
	phasync_spawn sp;

	if (!phasync_reading() || EG(active_fiber) == NULL) {
		orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	sp.n = phasync_children(sp.before);
	PHASYNC_G(spawn) = &sp;
	orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
	PHASYNC_G(spawn) = NULL;             /* never left armed, whatever happened */
}

static ZEND_NAMED_FUNCTION(phasync_exec_override)
{
	phasync_exec_family(INTERNAL_FUNCTION_PARAM_PASSTHRU, PHASYNC_G(orig_exec));
}
static ZEND_NAMED_FUNCTION(phasync_system_override)
{
	phasync_exec_family(INTERNAL_FUNCTION_PARAM_PASSTHRU, PHASYNC_G(orig_system));
}
static ZEND_NAMED_FUNCTION(phasync_passthru_override)
{
	phasync_exec_family(INTERNAL_FUNCTION_PARAM_PASSTHRU, PHASYNC_G(orig_passthru));
}
static ZEND_NAMED_FUNCTION(phasync_shell_exec_override)
{
	phasync_exec_family(INTERNAL_FUNCTION_PARAM_PASSTHRU, PHASYNC_G(orig_shell_exec));
}

/* popen() streams are wrapped like proc_open() pipes, and tainted. */
static ZEND_NAMED_FUNCTION(phasync_popen_override)
{
	phasync_spawn sp;
	php_stream *s = NULL;

	sp.n = phasync_children(sp.before);
	PHASYNC_G(orig_popen)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
	if (Z_TYPE_P(return_value) != IS_RESOURCE) {
		return;
	}
	php_stream_from_zval_no_verify(s, return_value);
	if (s) {
		phasync_entry_ensure(s)->pidfd = phasync_new_child_pidfd(&sp);
		phasync_wrap_stream(s, PHASYNC_MODE_RAW);
	}
}

/* ext/standard's php_process_handle: its leading fields, the same from PHP 8.2 to
 * master on POSIX. */
typedef struct {
	pid_t           child;
	int             npipes;
	zend_resource **pipes;
} phasync_proc_handle;

static ZEND_NAMED_FUNCTION(phasync_proc_close_override)
{
	int dir = phasync_reading();
	zval *zproc;
	const char *type;
	phasync_proc_handle *proc;
	siginfo_t si;
	int pidfd, i, w;

	if (!dir || EG(active_fiber) == NULL || ZEND_NUM_ARGS() != 1
	 || Z_TYPE_P(zproc = ZEND_CALL_ARG(execute_data, 1)) != IS_RESOURCE
	 || (type = zend_rsrc_list_get_rsrc_type(Z_RES_P(zproc))) == NULL || strcmp(type, "process") != 0) {
		PHASYNC_G(orig_proc_close)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	proc = Z_RES_P(zproc)->ptr;
	/* First close the pipes, as the native close does, so a child waiting for EOF
	 * on its stdin can exit. */
	for (i = 0; i < proc->npipes; i++) {
		if (proc->pipes[i] != NULL) {
			GC_DELREF(proc->pipes[i]);
			zend_list_close(proc->pipes[i]);
			proc->pipes[i] = NULL;
		}
	}
	/* waitid(WNOWAIT) checks the pid is still our child (proc_get_status() may have
	 * reaped it); then the pidfd turns readable when it exits, and the original
	 * reaps it at once. */
	pidfd = (int) syscall(SYS_pidfd_open, proc->child, 0);
	if (pidfd >= 0) {
		w = waitid(P_PID, proc->child, &si, WEXITED | WNOHANG | WNOWAIT) == 0
			? phasync_wait_fd(dir, NULL, pidfd, INFINITY) : PHASYNC_WAIT_READY;
		close(pidfd);
		if (w == PHASYNC_WAIT_ERROR) {
			return;                      /* exception pending: propagate */
		}
	}
	PHASYNC_G(orig_proc_close)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

/* pcntl_waitpid()/pcntl_wait() block until a child changes state. Inside a
 * scope, in a fiber, for an exit (no WNOHANG/WUNTRACED/WCONTINUED): a given pid
 * that is still an unreaped child of ours is waited for on a pidfd, readable
 * once it exits; any child or a process group (pid <= 0) has no pidfd, so it is
 * checked without reaping (waitid(WNOWAIT)) and slept on between checks (1ms,
 * doubling to 20ms). Then the original reaps at once, with the native status
 * and resource usage. */
static void phasync_waitpid_common(INTERNAL_FUNCTION_PARAMETERS, zend_long pid, zend_long flags,
                                   void (*orig)(INTERNAL_FUNCTION_PARAMETERS))
{
	siginfo_t si;
	int pidfd;

	if (!phasync_reading() || EG(active_fiber) == NULL
	 || (flags & (WNOHANG | WUNTRACED
#ifdef WCONTINUED
	              | WCONTINUED
#endif
	 ))) {
		orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	if (pid > 0) {
		if ((pidfd = (int) syscall(SYS_pidfd_open, (pid_t) pid, 0)) >= 0) {
			int w = PHASYNC_WAIT_READY;
			memset(&si, 0, sizeof(si));
			if (waitid(P_PID, (id_t) pid, &si, WEXITED | WNOHANG | WNOWAIT) == 0 && si.si_pid == 0) {
				w = phasync_wait_fd(PHASYNC_READ, NULL, pidfd, INFINITY);   /* not exited yet */
			}
			close(pidfd);
			if (w != PHASYNC_WAIT_READY) {
				return;                      /* exception pending */
			}
		}
	} else {
		idtype_t type = pid == -1 ? P_ALL : P_PGID;
		id_t id = pid == -1 ? 0 : pid == 0 ? (id_t) getpgrp() : (id_t) -pid;
		zend_long usec = 1000;
		for (;;) {
			memset(&si, 0, sizeof(si));
			if (waitid(type, id, &si, WEXITED | WNOHANG | WNOWAIT) != 0 || si.si_pid != 0) {
				break;                       /* one exited, or none to wait for (ECHILD) */
			}
			if (phasync_call_sleep(phasync_sleep_handler(), usec) < 0) {
				return;                      /* exception pending */
			}
			usec = MIN(usec * 2, 20000);
		}
	}
	orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static ZEND_NAMED_FUNCTION(phasync_pcntl_waitpid_override)
{
	uint32_t argc = ZEND_NUM_ARGS();
	zval *zpid = argc >= 1 ? ZEND_CALL_ARG(execute_data, 1) : NULL;
	zval *zflags = argc >= 3 ? ZEND_CALL_ARG(execute_data, 3) : NULL;

	if (!zpid || Z_TYPE_P(zpid) != IS_LONG || (zflags && Z_TYPE_P(zflags) != IS_LONG)) {
		PHASYNC_G(orig_pcntl_waitpid)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	phasync_waitpid_common(INTERNAL_FUNCTION_PARAM_PASSTHRU, Z_LVAL_P(zpid), zflags ? Z_LVAL_P(zflags) : 0,
		PHASYNC_G(orig_pcntl_waitpid));
}

static ZEND_NAMED_FUNCTION(phasync_pcntl_wait_override)
{
	uint32_t argc = ZEND_NUM_ARGS();
	zval *zflags = argc >= 2 ? ZEND_CALL_ARG(execute_data, 2) : NULL;

	if (zflags && Z_TYPE_P(zflags) != IS_LONG) {
		PHASYNC_G(orig_pcntl_wait)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	phasync_waitpid_common(INTERNAL_FUNCTION_PARAM_PASSTHRU, -1, zflags ? Z_LVAL_P(zflags) : 0,
		PHASYNC_G(orig_pcntl_wait));
}

/* ---- filesystem functions --------------------------------------------------
 *
 * Metadata and namespace calls are fast on a local disk and would only get slower
 * through the pool (~30µs+ a call vs ~1µs; autoloaders make thousands), so by
 * default only paths on network/FUSE mounts are offloaded, where a call can stall
 * for a round trip or worse (phasync.fs_offload=all offloads everything, =none
 * nothing). Read-only calls stat (or read) the path on the pool, warming the
 * kernel's caches, and then run the original, which answers from them — results,
 * PHP's stat cache and warnings stay native. unlink/rmdir/mkdir/rename run on the
 * pool; on failure the original runs to fail again with the native warning. */

static bool phasync_fs_slow_type(const char *type)
{
	static const char *const slow[] = {
		"nfs", "nfs4", "cifs", "smb3", "smbfs", "9p", "ceph", "glusterfs", "lustre",
		"gpfs", "beegfs", "afs", "virtiofs", "davfs", NULL
	};
	const char *const *s;

	if (strcmp(type, "fuse") == 0 || strcmp(type, "fuseblk") == 0 || strncmp(type, "fuse.", 5) == 0) {
		return true;                     /* fuse.sshfs, fuse.s3fs, ...: userland */
	}
	for (s = slow; *s; s++) {
		if (strcmp(type, *s) == 0) {
			return true;
		}
	}
	return false;
}

/* Is this type listed in phasync.fs_offload_types (comma-separated)? */
static bool phasync_fs_listed_type(const char *type)
{
	const char *l = PHASYNC_G(fs_offload_types);
	size_t tl = strlen(type);

	while (l && *l) {
		l += strspn(l, " ,");
		size_t n = strcspn(l, " ,");
		if (n == tl && n && strncmp(l, type, n) == 0) {
			return true;
		}
		l += n;
	}
	return false;
}

static void phasync_mounts_free(void)
{
	for (int i = 0; i < PHASYNC_G(nmounts); i++) {
		free(PHASYNC_G(mounts)[i].path);
	}
	free(PHASYNC_G(mounts));
	PHASYNC_G(mounts) = NULL;
	PHASYNC_G(nmounts) = 0;
	PHASYNC_G(any_slow_mount) = false;
	PHASYNC_G(any_pool_mount) = false;
	free(PHASYNC_G(nowait_off));         /* a device number may now be another filesystem */
	PHASYNC_G(nowait_off) = NULL;
	PHASYNC_G(nnowait_off) = 0;
	PHASYNC_G(fs_last) = NULL;
}

/* (Re)load the mount table when it changed: /proc/self/mountinfo polls POLLPRI
 * after a mount or unmount. Polled at most once a second (the clock is read in
 * the vDSO, no syscall), so a filesystem call costs no extra syscall and a new
 * mount is noticed within a second. Lines look like
 *   36 35 98:0 /mnt1 /mnt/parent rw,noatime master:1 - ext3 /dev/root rw */
static void phasync_mounts_refresh(void)
{
	int fd = PHASYNC_G(mountinfo_fd);
	char *buf = NULL, *line, *next;
	size_t size = 0, len = 0;
	ssize_t n;

	if (fd == -2) {
		return;                          /* no /proc: nothing counts as slow */
	}
	if (fd == -1) {
		if ((fd = open("/proc/self/mountinfo", O_RDONLY | O_CLOEXEC)) < 0) {
			PHASYNC_G(mountinfo_fd) = -2;
			return;
		}
		PHASYNC_G(mountinfo_fd) = fd;
	} else {
		struct pollfd pfd = { fd, POLLPRI, 0 };
		struct timespec now;
		clock_gettime(CLOCK_MONOTONIC_COARSE, &now);
		if (now.tv_sec == PHASYNC_G(mounts_checked)) {
			return;                      /* polled this second already */
		}
		PHASYNC_G(mounts_checked) = now.tv_sec;
		if (poll(&pfd, 1, 0) <= 0 || !(pfd.revents & (POLLPRI | POLLERR))) {
			return;                      /* unchanged */
		}
	}
	lseek(fd, 0, SEEK_SET);
	do {
		if (len + 4096 > size) {
			char *nb = realloc(buf, size = size ? size * 2 : 16384);
			if (nb == NULL) {
				free(buf);
				return;
			}
			buf = nb;
		}
		n = read(fd, buf + len, size - len - 1);
		len += n > 0 ? (size_t) n : 0;
	} while (n > 0);
	buf[len] = '\0';

	phasync_mounts_free();
	for (line = buf; *line; line = next) {
		char *mnt = line, *end, *type, *o, *i;
		unsigned int major = 0, minor = 0;
		int field;

		next = strchr(line, '\n');
		next = next ? (*next = '\0', next + 1) : line + strlen(line);
		for (field = 0; field < 4 && mnt; field++) {
			mnt = strchr(mnt, ' ');
			mnt = mnt ? mnt + 1 : NULL;
		}
		if (!mnt || !(end = strchr(mnt, ' ')) || !(type = strstr(end, " - "))
		 || sscanf(line, "%*s %*s %u:%u", &major, &minor) != 2) {
			continue;
		}
		*end = '\0';
		type += 3;
		if ((o = strchr(type, ' '))) {
			*o = '\0';
		}
		for (i = o = mnt; *i; o++) {     /* unescape \ooo (spaces etc.) */
			if (i[0] == '\\' && i[1] >= '0' && i[1] <= '3' && i[2] >= '0' && i[2] <= '7' && i[3] >= '0' && i[3] <= '7') {
				*o = (char) (((i[1] - '0') << 6) | ((i[2] - '0') << 3) | (i[3] - '0'));
				i += 4;
			} else {
				*o = *i++;
			}
		}
		*o = '\0';
		if (PHASYNC_G(nmounts) % 64 == 0) {
			phasync_mount *nm = realloc(PHASYNC_G(mounts), (PHASYNC_G(nmounts) + 64) * sizeof(*nm));
			if (nm == NULL) {
				break;
			}
			PHASYNC_G(mounts) = nm;
		}
		phasync_mount *m = &PHASYNC_G(mounts)[PHASYNC_G(nmounts)++];
		m->path = strdup(mnt);
		m->len = strlen(mnt);
		m->dev = makedev(major, minor);
		m->slow = phasync_fs_slow_type(type);
		m->pool = m->slow || phasync_fs_listed_type(type);
		PHASYNC_G(any_slow_mount) |= m->slow;
		PHASYNC_G(any_pool_mount) |= m->pool;
	}
	free(buf);

	/* A lookup can only come out slow on a slow mount or one at or under it (that
	 * shadows it): keep just those, in order, so a lookup scans a handful; and the
	 * pool mounts, matched by device. */
	int kept = 0;
	for (int i = 0; i < PHASYNC_G(nmounts); i++) {
		phasync_mount *m = &PHASYNC_G(mounts)[i];
		bool keep = m->pool;
		for (int j = 0; j < PHASYNC_G(nmounts) && !keep; j++) {
			phasync_mount *s = &PHASYNC_G(mounts)[j];
			keep = s->slow && strncmp(m->path, s->path, s->len) == 0
				&& (m->path[s->len] == '/' || m->path[s->len] == '\0' || s->len == 1);
		}
		if (keep) {
			PHASYNC_G(mounts)[kept++] = *m;
		} else {
			free(m->path);
		}
	}
	PHASYNC_G(nmounts) = kept;
}

/* Is this absolute path offloaded? Under the network policy: when the mount it
 * lies on (lexically, the longest matching mount point; the later of stacked
 * ones) is a network/FUSE filesystem. */
static bool phasync_fs_offloaded(const char *path)
{
	int best = -1;
	size_t bestlen = 0;

	if (PHASYNC_G(fs_offload) != PHASYNC_FS_OFFLOAD_NETWORK) {
		return PHASYNC_G(fs_offload) == PHASYNC_FS_OFFLOAD_ALL;
	}
	phasync_mounts_refresh();
	if (!PHASYNC_G(any_slow_mount)) {
		return false;
	}
	for (int i = 0; i < PHASYNC_G(nmounts); i++) {
		phasync_mount *m = &PHASYNC_G(mounts)[i];
		if (m->len >= bestlen && strncmp(path, m->path, m->len) == 0
		 && (path[m->len] == '/' || path[m->len] == '\0' || m->len == 1)) {
			best = i;
			bestlen = m->len;
		}
	}
	return best >= 0 && PHASYNC_G(mounts)[best].slow;
}

/* How do reads of this descriptor run in a coroutine (PHASYNC_FS_*)? A regular
 * file on a network/FUSE mount, or on a filesystem type listed in
 * phasync.fs_offload_types, reads on the pool (=all: every regular file). Any
 * other is read with RWF_NOWAIT, inline from the page cache and on the pool only
 * when it would wait for the disk, unless its filesystem refused RWF_NOWAIT
 * (ZFS, for one): then inline, as natively. While no such mount is known, this
 * costs no syscall; the reader tells a regular file only when a read would
 * block. */
static int phasync_fs_fd_mode(php_socket_t fd)
{
	struct stat st;

	if (PHASYNC_G(fs_offload) == PHASYNC_FS_OFFLOAD_NONE) {
		return PHASYNC_FS_INLINE;
	}
	if (PHASYNC_G(fs_offload) == PHASYNC_FS_OFFLOAD_NETWORK) {
		phasync_mounts_refresh();
		if (!PHASYNC_G(any_pool_mount) && PHASYNC_G(nnowait_off) == 0) {
			return PHASYNC_FS_NOWAIT;
		}
	}
	if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
		return PHASYNC_FS_INLINE;
	}
	if (PHASYNC_G(fs_offload) == PHASYNC_FS_OFFLOAD_ALL) {
		return PHASYNC_FS_POOL;
	}
	for (int i = 0; i < PHASYNC_G(nmounts); i++) {
		if (PHASYNC_G(mounts)[i].pool && PHASYNC_G(mounts)[i].dev == st.st_dev) {
			return PHASYNC_FS_POOL;
		}
	}
	for (int i = 0; i < PHASYNC_G(nnowait_off); i++) {
		if (PHASYNC_G(nowait_off)[i] == st.st_dev) {
			return PHASYNC_FS_INLINE;
		}
	}
	return PHASYNC_FS_NOWAIT;
}

static bool phasync_fs_fd_offloaded(php_socket_t fd)
{
	return phasync_fs_fd_mode(fd) == PHASYNC_FS_POOL;
}

/* Copy a plain local path argument into out (PATH_MAX), made absolute. Returns
 * 1 if it is offloaded, 0 if not, -1 if it isn't a plain local path (a stream
 * wrapper, an embedded NUL, too long: then the original handles it). */
static int phasync_fs_path(zval *z, char *out)
{
	size_t len, cwdlen = 0;

	if (Z_TYPE_P(z) != IS_STRING || (len = Z_STRLEN_P(z)) == 0
	 || strlen(Z_STRVAL_P(z)) != len || strstr(Z_STRVAL_P(z), "://") != NULL) {
		return -1;
	}
	if (Z_STRVAL_P(z)[0] != '/') {
		if (VCWD_GETCWD(out, PATH_MAX) == NULL) {
			return -1;
		}
		cwdlen = strlen(out);
		out[cwdlen++] = '/';
	}
	if (cwdlen + len >= PATH_MAX) {
		return -1;
	}
	memcpy(out + cwdlen, Z_STRVAL_P(z), len + 1);
	return phasync_fs_offloaded(out) ? 1 : 0;
}

static ZEND_NAMED_FUNCTION(phasync_fs_override)
{
	/* Keyed by the name, not the function: a closure of it (is_file(...),
	 * Closure::fromCallable()) calls a copy that shares the name (#9). */
	zval *idx = zend_hash_index_find(&PHASYNC_G(fs_hooks), (zend_ulong) (uintptr_t) EX(func)->common.function_name);
	zif_handler orig = PHASYNC_G(orig_fs)[Z_LVAL_P(idx)];
	uint32_t argc = ZEND_NUM_ARGS();
	phasync_task t;
	char *p;

	t.fsop = phasync_fs_funcs[Z_LVAL_P(idx)].op;
	if (!phasync_reading() || EG(active_fiber) == NULL || argc < 1) {
		orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	switch (t.fsop) {
		case PHASYNC_FS_WARM:
		case PHASYNC_FS_WARM_DIR:
			if (phasync_fs_path(ZEND_CALL_ARG(execute_data, 1), t.path) != 1) {
				break;
			}
			t.type = PHASYNC_OP_FS;
			phasync_pool_run(&t);
			break;
		case PHASYNC_FS_WARM_GLOB:
			if (phasync_fs_path(ZEND_CALL_ARG(execute_data, 1), t.path) != 1) {
				break;
			}
			t.path[strcspn(t.path, "*?[{")] = '\0';   /* the directory before the first wildcard */
			*(strrchr(t.path, '/') + (strrchr(t.path, '/') == t.path)) = '\0';
			t.type = PHASYNC_OP_FS;
			phasync_pool_run(&t);
			break;
		case PHASYNC_FS_UNLINK:
		case PHASYNC_FS_RMDIR:
		case PHASYNC_FS_MKDIR:
		case PHASYNC_FS_RENAME: {
			/* No stream context, plain argument types, and no open_basedir refusal
			 * (the original reports that); anything else is left to the original. */
			uint32_t nargs = t.fsop == PHASYNC_FS_RENAME ? 2 : t.fsop == PHASYNC_FS_MKDIR ? 3 : 1;
			zval *ctx = argc > nargs ? ZEND_CALL_ARG(execute_data, nargs + 1) : NULL;
			int slow;

			if ((ctx && Z_TYPE_P(ctx) != IS_NULL)
			 || (slow = phasync_fs_path(ZEND_CALL_ARG(execute_data, 1), t.path)) < 0
			 || (PG(open_basedir) && *PG(open_basedir) && php_check_open_basedir_ex(t.path, 0))) {
				break;
			}
			if (t.fsop == PHASYNC_FS_RENAME) {
				int slow2;
				if (argc < 2 || (slow2 = phasync_fs_path(ZEND_CALL_ARG(execute_data, 2), t.path2)) < 0
				 || (PG(open_basedir) && *PG(open_basedir) && php_check_open_basedir_ex(t.path2, 0))) {
					break;
				}
				slow |= slow2;
			}
			if (t.fsop == PHASYNC_FS_MKDIR) {
				zval *mode = argc >= 2 ? ZEND_CALL_ARG(execute_data, 2) : NULL;
				zval *rec  = argc >= 3 ? ZEND_CALL_ARG(execute_data, 3) : NULL;
				if ((mode && Z_TYPE_P(mode) != IS_LONG) || (rec && Z_TYPE_P(rec) != IS_TRUE && Z_TYPE_P(rec) != IS_FALSE)) {
					break;
				}
				t.omode = mode ? (int) Z_LVAL_P(mode) : 0777;
				if (rec && Z_TYPE_P(rec) == IS_TRUE) {
					t.fsop = PHASYNC_FS_MKDIR_P;
					for (p = t.path + strlen(t.path) - 1; p > t.path && *p == '/'; p--) {
						*p = '\0';               /* mkdir -p a/b/ creates a/b */
					}
				}
			}
			if (!slow) {
				break;
			}
			t.type = PHASYNC_OP_FS;
			phasync_pool_run(&t);
			if (EG(exception)) {
				return;
			}
			if (t.result == 0) {
				if (t.fsop != PHASYNC_FS_MKDIR && t.fsop != PHASYNC_FS_MKDIR_P) {
					php_clear_stat_cache(1, NULL, 0);   /* as the native op does */
				}
				RETURN_TRUE;
			}
			break;                       /* failed: the original reports it natively */
		}
		default:
			break;
	}
	if (EG(exception)) {
		return;
	}
	orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

/* ---- echo/print to the CLI's stdout -----------------------------------------
 *
 * The CLI SAPI writes output straight to fd 1, so echo blocks the whole process
 * on a full pipe (a slow log reader). Inside a scope, in a fiber, wait for fd 1 to
 * be writable and let the SAPI write at most PIPE_BUF bytes at a time, which then
 * can't block. Another fiber's output meanwhile waits its turn, so each echo stays
 * contiguous as it is natively. */
static size_t (*phasync_ub_write_orig)(const char *str, size_t len);

static size_t phasync_ub_write(const char *str, size_t len)
{
	int dir = phasync_writing();
	size_t done = 0, n;

	if (!dir || EG(active_fiber) == NULL) {
		return phasync_ub_write_orig(str, len);
	}
	while (PHASYNC_G(ub_writing)) {
		if (phasync_call_sleep(phasync_sleep_handler(), 1000) < 0) {
			return 0;                    /* exception pending */
		}
	}
	PHASYNC_G(ub_writing) = true;
	while (done < len) {
		struct pollfd pfd = { STDOUT_FILENO, POLLOUT, 0 };
		if (poll(&pfd, 1, 0) == 0) {
			if (phasync_wait_fd(dir, NULL, STDOUT_FILENO, INFINITY) != PHASYNC_WAIT_READY) {
				break;
			}
			continue;
		}
		if ((n = phasync_ub_write_orig(str + done, MIN(len - done, PIPE_BUF))) == 0) {
			break;
		}
		done += n;
	}
	PHASYNC_G(ub_writing) = false;
	return done;
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

	if (PHASYNC_G(select_depth) || !phasync_reading() || EG(active_fiber) == NULL) {
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
			w = phasync_wait_fd(PHASYNC_READ, NULL, epfd, remaining);
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

/* ---- ext/sockets: socket_read() & co. -------------------------------------
 *
 * Socket objects are not streams, so their blocking calls are overridden one by
 * one. Inside a scope, in a fiber, on a socket left blocking: if it is not ready,
 * park until it is (its SO_RCVTIMEO/SO_SNDTIMEO, if set, as the native timeout),
 * then let the original do the call, which then doesn't wait; the real call
 * stays on PHP's thread, so a cancelled wait consumes nothing. When the native
 * timeout runs out, the original runs once non-blocking, failing with PHP's own
 * warning and socket error, as the native timeout does. socket_read() in
 * PHP_NORMAL_READ mode waits cooperatively for the first byte; the rest of the
 * line is read by the original. */

static const struct { const char *name; int dir; int timeo; } phasync_sock_funcs[] = {
	{"socket_read", PHASYNC_READ, SO_RCVTIMEO}, {"socket_recv", PHASYNC_READ, SO_RCVTIMEO},
	{"socket_recvfrom", PHASYNC_READ, SO_RCVTIMEO}, {"socket_accept", PHASYNC_READ, SO_RCVTIMEO},
	{"socket_write", PHASYNC_WRITE, SO_SNDTIMEO}, {"socket_send", PHASYNC_WRITE, SO_SNDTIMEO},
	{"socket_sendto", PHASYNC_WRITE, SO_SNDTIMEO},
};
#define PHASYNC_SOCK_NFUNCS (sizeof(phasync_sock_funcs) / sizeof(phasync_sock_funcs[0]))

/* The Socket argument's fd if it is a blocking socket we may wait on, else -1. */
static php_socket_t phasync_socket_arg(zend_execute_data *execute_data, phasync_php_socket **out)
{
	zval *z;
	php_socket_t fd;

	if (ZEND_NUM_ARGS() < 1 || (fd = phasync_select_fd_socket(z = ZEND_CALL_ARG(execute_data, 1))) == -1) {
		return -1;
	}
	ZVAL_DEREF(z);
	*out = (phasync_php_socket *) ((char *) Z_OBJ_P(z) - XtOffsetOf(phasync_php_socket, std));
	return (*out)->blocking ? fd : -1;
}

static ZEND_NAMED_FUNCTION(phasync_socket_io_override)
{
	/* Keyed by the name, not the function: a closure of it (socket_read(...),
	 * Closure::fromCallable()) calls a copy that shares the name (#9). */
	zval *idx = zend_hash_index_find(&PHASYNC_G(sock_hooks), (zend_ulong) (uintptr_t) EX(func)->common.function_name);
	zif_handler orig = PHASYNC_G(orig_sock)[Z_LVAL_P(idx)];
	int dir = phasync_sock_funcs[Z_LVAL_P(idx)].dir;
	phasync_php_socket *ps;
	php_socket_t fd;
	struct pollfd pfd;

	if (!phasync_reading() || EG(active_fiber) == NULL || (fd = phasync_socket_arg(execute_data, &ps)) == -1) {
		orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	pfd.fd = fd;
	pfd.events = dir == PHASYNC_READ ? POLLIN : POLLOUT;
	if (poll(&pfd, 1, 0) == 0) {
		struct timeval tv = {0, 0};
		socklen_t len = sizeof(tv);
		double timeout = INFINITY, deadline;
		int w;

		if (getsockopt(fd, SOL_SOCKET, phasync_sock_funcs[Z_LVAL_P(idx)].timeo, &tv, &len) == 0
		 && (tv.tv_sec || tv.tv_usec)) {
			timeout = (double) tv.tv_sec + tv.tv_usec / 1e6;
		}
		deadline = phasync_now() + timeout;
		do {
			w = phasync_wait_fd(dir, NULL, fd, isinf(timeout) ? INFINITY : MAX(deadline - phasync_now(), 0));
		} while (w == PHASYNC_WAIT_READY && poll(&pfd, 1, 0) == 0);
		if (w == PHASYNC_WAIT_ERROR) {
			return;                          /* exception pending */
		}
		if (w == PHASYNC_WAIT_TIMEOUT) {
			int flags = fcntl(fd, F_GETFL, 0);
			fcntl(fd, F_SETFL, flags | O_NONBLOCK);
			orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);   /* fails with the native EAGAIN */
			fcntl(fd, F_SETFL, flags);
			return;
		}
	}
	orig(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

/* What ext/sockets' PHP_SOCKET_ERROR() does: the socket's error, the module's
 * last error (the first field of its globals, the same from 8.2 to master; not
 * reachable this way in a ZTS build) and the warning. */
static void phasync_socket_error(phasync_php_socket *ps, const char *msg, int err)
{
	ps->error = err;
#ifndef ZTS
	{
		zend_module_entry *m = zend_hash_str_find_ptr(&module_registry, "sockets", sizeof("sockets") - 1);
		if (m && m->globals_ptr) {
			*(int *) m->globals_ptr = err;
		}
	}
#endif
	if (err != EAGAIN && err != EWOULDBLOCK && err != EINPROGRESS) {
		php_error_docref(NULL, E_WARNING, "%s [%d]: %s", msg, err, strerror(err));
	}
}

/* socket_close() while another coroutine waits on the socket: epoll forgets a
 * closed descriptor, so wake the waiters first (as a stream's close op does);
 * their retry finds the Socket closed, as PHP reports it. */
static ZEND_NAMED_FUNCTION(phasync_socket_close_override)
{
	zval *z = ZEND_NUM_ARGS() >= 1 ? ZEND_CALL_ARG(execute_data, 1) : NULL;
	php_socket_t fd;

	if (z && (fd = phasync_select_fd_socket(z)) != -1) {
		for (phasync_poller *p = phasync_pollers; p; p = p->next) {
			phasync_reg *r = p->fork_gen == phasync_fork_gen ? zend_hash_index_find_ptr(&p->regs, (zend_ulong) fd) : NULL;
			if (r == NULL || r->stream != NULL) {
				continue;
			}
			for (int i = 0; i < 2; i++) {
				if (r->slot[i] >= 0) {
					phasync_chan_push(p->chan, r->slot[i]);
					r->slot[i] = -1;
					p->armed--;
				}
			}
			phasync_reg_drop(p, r);
		}
	}
	PHASYNC_G(orig_socket_close)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

/* socket_connect() to a numeric IPv4/IPv6 address or a unix path: connect
 * non-blocking and park until the socket is writable; true on success, false
 * with ext/sockets' own error reporting otherwise, as the original does. A host
 * name goes to the original (its lookup is PHP's internal blocking one). */
static ZEND_NAMED_FUNCTION(phasync_socket_connect_override)
{
	uint32_t argc = ZEND_NUM_ARGS();
	zval *zaddr = argc >= 2 ? ZEND_CALL_ARG(execute_data, 2) : NULL;
	zval *zport = argc >= 3 ? ZEND_CALL_ARG(execute_data, 3) : NULL;
	struct sockaddr_storage ss;
	socklen_t sslen = 0;
	phasync_php_socket *ps;
	php_socket_t fd;
	int flags, rc, err = 0;

	memset(&ss, 0, sizeof(ss));
	if (!phasync_reading() || EG(active_fiber) == NULL || (fd = phasync_socket_arg(execute_data, &ps)) == -1
	 || !zaddr || Z_TYPE_P(zaddr) != IS_STRING || (zport && Z_TYPE_P(zport) != IS_LONG && Z_TYPE_P(zport) != IS_NULL)) {
		PHASYNC_G(orig_socket_connect)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	if (ps->type == AF_INET && zport && Z_TYPE_P(zport) == IS_LONG
	 && inet_pton(AF_INET, Z_STRVAL_P(zaddr), &((struct sockaddr_in *) &ss)->sin_addr) == 1) {
		((struct sockaddr_in *) &ss)->sin_family = AF_INET;
		((struct sockaddr_in *) &ss)->sin_port = htons((unsigned short) Z_LVAL_P(zport));
		sslen = sizeof(struct sockaddr_in);
	} else if (ps->type == AF_INET6 && zport && Z_TYPE_P(zport) == IS_LONG
	        && inet_pton(AF_INET6, Z_STRVAL_P(zaddr), &((struct sockaddr_in6 *) &ss)->sin6_addr) == 1) {
		((struct sockaddr_in6 *) &ss)->sin6_family = AF_INET6;
		((struct sockaddr_in6 *) &ss)->sin6_port = htons((unsigned short) Z_LVAL_P(zport));
		sslen = sizeof(struct sockaddr_in6);
	} else if (ps->type == AF_UNIX && Z_STRLEN_P(zaddr) < sizeof(((struct sockaddr_un *) &ss)->sun_path)) {
		((struct sockaddr_un *) &ss)->sun_family = AF_UNIX;
		memcpy(((struct sockaddr_un *) &ss)->sun_path, Z_STRVAL_P(zaddr), Z_STRLEN_P(zaddr));
		sslen = (socklen_t) (XtOffsetOf(struct sockaddr_un, sun_path) + Z_STRLEN_P(zaddr));
	}
	if (sslen == 0) {
		PHASYNC_G(orig_socket_connect)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	do { rc = connect(fd, (struct sockaddr *) &ss, sslen); } while (rc != 0 && errno == EINTR);
	err = rc != 0 ? errno : 0;
	if (rc != 0 && errno == EINPROGRESS) {
		socklen_t elen = sizeof(err);
		if (phasync_wait_fd(PHASYNC_WRITE, NULL, fd, INFINITY) != PHASYNC_WAIT_READY) {
			fcntl(fd, F_SETFL, flags);
			return;                          /* exception pending */
		}
		if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen) != 0) {
			err = errno;
		}
		rc = err == 0 ? 0 : -1;
	}
	fcntl(fd, F_SETFL, flags);
	if (rc == 0) {
		RETURN_TRUE;
	}
	phasync_socket_error(ps, "unable to connect", err);
	RETURN_FALSE;
}

/* curl_multi_select() waits for curl's own sockets, which PHP doesn't expose
 * (no curl_multi_fdset()). Inside a scope, in a fiber: probe the original with a
 * zero timeout, and sleep through the loop between probes (1ms, doubling to
 * 20ms) until activity or the caller's timeout; 0 on timeout, as natively. The
 * probe calls the function again by name, which (select_depth > 0) goes
 * straight to the original. */
static ZEND_NAMED_FUNCTION(phasync_curl_multi_select_override)
{
	uint32_t argc = ZEND_NUM_ARGS();
	zval *zmh = argc >= 1 ? ZEND_CALL_ARG(execute_data, 1) : NULL;
	zval *zto = argc >= 2 ? ZEND_CALL_ARG(execute_data, 2) : NULL;
	double timeout = 1.0, deadline;
	zend_long usec = 1000;
	zval fn, args[2], ret;

	if (PHASYNC_G(select_depth) || !phasync_reading() || EG(active_fiber) == NULL || !zmh
	 || (zto && Z_TYPE_P(zto) != IS_DOUBLE && Z_TYPE_P(zto) != IS_LONG)) {
		PHASYNC_G(orig_curl_multi_select)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	if (zto) {
		timeout = zval_get_double(zto);
	}
	deadline = phasync_now() + timeout;
	ZVAL_STRING(&fn, "curl_multi_select");
	ZVAL_COPY(&args[0], zmh);
	ZVAL_DOUBLE(&args[1], 0.0);
	for (;;) {
		double left;
		ZVAL_UNDEF(&ret);
		PHASYNC_G(select_depth)++;
		call_user_function(NULL, NULL, &fn, &ret, 2, args);
		PHASYNC_G(select_depth)--;
		if (EG(exception) || Z_TYPE(ret) != IS_LONG || Z_LVAL(ret) != 0
		 || (left = deadline - phasync_now()) <= 0) {
			break;                           /* activity, an error (-1), or time up */
		}
		zval_ptr_dtor(&ret);
		if (phasync_call_sleep(phasync_sleep_handler(), MIN(usec, (zend_long) (left * 1e6) + 1)) < 0) {
			ZVAL_UNDEF(&ret);
			break;                           /* exception pending */
		}
		usec = MIN(usec * 2, 20000);
	}
	zval_ptr_dtor(&fn);
	zval_ptr_dtor(&args[0]);
	if (!Z_ISUNDEF(ret)) {
		RETURN_COPY_VALUE(&ret);
	}
}

/* sem_acquire() and msg_receive() block in the kernel until the semaphore or a
 * message is there, with nothing to wait on. Inside a scope, in a fiber, their
 * non-blocking forms are retried from the loop instead (1ms, doubling to 20ms
 * between tries), so nothing is taken except on PHP's thread and a cancelled
 * wait takes nothing. The function is called again by name (select_depth > 0:
 * straight to the original) with the caller's arguments, references included. */
static bool phasync_call_again(const char *name, zval *retval, uint32_t argc, zval *args)
{
	zval fn;
	bool ok;

	ZVAL_STRING(&fn, name);
	ZVAL_UNDEF(retval);
	PHASYNC_G(select_depth)++;
	ok = call_user_function(NULL, NULL, &fn, retval, argc, args) == SUCCESS && !EG(exception);
	PHASYNC_G(select_depth)--;
	zval_ptr_dtor(&fn);
	return ok;
}

/* sem_acquire($sem) -> sem_acquire($sem, true) until it succeeds; EAGAIN (taken)
 * is the only failure that is retried (the native non-blocking form reports it
 * without a warning); any other failure returns as natively. */
static ZEND_NAMED_FUNCTION(phasync_sem_acquire_override)
{
	uint32_t argc = ZEND_NUM_ARGS();
	zval args[2], ret;
	zend_long usec = 1000;

	if (PHASYNC_G(select_depth) || !phasync_reading() || EG(active_fiber) == NULL || argc < 1
	 || (argc >= 2 && zend_is_true(ZEND_CALL_ARG(execute_data, 2)))) {
		PHASYNC_G(orig_sem_acquire)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	ZVAL_COPY_VALUE(&args[0], ZEND_CALL_ARG(execute_data, 1));
	ZVAL_TRUE(&args[1]);
	for (;;) {
		errno = 0;
		if (!phasync_call_again("sem_acquire", &ret, 2, args) || Z_TYPE(ret) != IS_FALSE || errno != EAGAIN) {
			break;
		}
		if (phasync_call_sleep(phasync_sleep_handler(), usec) < 0) {
			return;                          /* exception pending */
		}
		usec = MIN(usec * 2, 20000);
	}
	if (!Z_ISUNDEF(ret)) {
		RETURN_COPY_VALUE(&ret);
	}
}

/* msg_receive(...) -> the same with MSG_IPC_NOWAIT until a message comes; "no
 * message" (ENOMSG in $error_code, no warning) is the only result retried. */
static ZEND_NAMED_FUNCTION(phasync_msg_receive_override)
{
	uint32_t argc = ZEND_NUM_ARGS(), i;
	zval args[8], ret, errcode, *uflags = argc >= 7 ? ZEND_CALL_ARG(execute_data, 7) : NULL;
	zend_long usec = 1000;
	bool own_err = argc < 8;

	if (PHASYNC_G(select_depth) || !phasync_reading() || EG(active_fiber) == NULL || argc < 5
	 || (uflags && (Z_TYPE_P(uflags) != IS_LONG || (Z_LVAL_P(uflags) & 1 /* MSG_IPC_NOWAIT */)))) {
		PHASYNC_G(orig_msg_receive)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		return;
	}
	for (i = 0; i < argc; i++) {
		ZVAL_COPY_VALUE(&args[i], ZEND_CALL_ARG(execute_data, i + 1));
	}
	if (argc < 6) {
		ZVAL_TRUE(&args[5]);                 /* $unserialize default */
	}
	ZVAL_LONG(&args[6], (uflags ? Z_LVAL_P(uflags) : 0) | 1);
	if (own_err) {
		ZVAL_NULL(&errcode);
		ZVAL_NEW_REF(&args[7], &errcode);
	}
	for (;;) {
		zval *code;
		if (!phasync_call_again("msg_receive", &ret, 8, args) || Z_TYPE(ret) != IS_FALSE) {
			break;
		}
		code = &args[7];
		ZVAL_DEREF(code);
		if (Z_TYPE_P(code) != IS_LONG || Z_LVAL_P(code) != ENOMSG) {
			break;
		}
		if (phasync_call_sleep(phasync_sleep_handler(), usec) < 0) {
			ZVAL_UNDEF(&ret);
			break;                           /* exception pending */
		}
		usec = MIN(usec * 2, 20000);
	}
	if (own_err) {
		zval_ptr_dtor(&args[7]);
	}
	if (!Z_ISUNDEF(ret)) {
		RETURN_COPY_VALUE(&ret);
	}
}

/* Call a PHP function by name (through any override of ours). */
static bool phasync_call_named(const char *name, zval *retval, uint32_t argc, zval *args)
{
	zval fn;
	bool ok;

	ZVAL_STRING(&fn, name);
	ZVAL_UNDEF(retval);
	ok = call_user_function(NULL, NULL, &fn, retval, argc, args) == SUCCESS && !EG(exception);
	zval_ptr_dtor(&fn);
	return ok;
}

/* curl_exec() inside a scope, in a fiber: the same transfer on a private
 * curl_multi handle, waiting in the cooperative curl_multi_select(), so option
 * callbacks run on PHP's thread, in the coroutine. The result is native:
 * curl_multi_info_read() records the transfer's error on the handle (curl_errno,
 * curl_error), adding the handle resets it as curl_exec() does, and the return is
 * false on error, the body with CURLOPT_RETURNTRANSFER, else true; FILE* outputs
 * are flushed, as curl_exec() does. A handle already in a multi (or a multi
 * error) goes to the original, which reports it natively. A cancellation drops
 * the transfer and propagates. */
static ZEND_NAMED_FUNCTION(phasync_curl_exec_override)
{
	zval mh, args[2], ret, running, info;
	zend_long result = 0;
	bool added = false;

	if (!phasync_reading() || EG(active_fiber) == NULL || ZEND_NUM_ARGS() != 1
	 || Z_TYPE_P(ZEND_CALL_ARG(execute_data, 1)) != IS_OBJECT
	 || !phasync_call_named("curl_multi_init", &mh, 0, NULL) || Z_TYPE(mh) != IS_OBJECT) {
		if (!EG(exception)) {
			PHASYNC_G(orig_curl_exec)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
		}
		return;
	}
	ZVAL_COPY_VALUE(&args[0], &mh);
	ZVAL_COPY_VALUE(&args[1], ZEND_CALL_ARG(execute_data, 1));
	if (!phasync_call_named("curl_multi_add_handle", &ret, 2, args) || Z_TYPE(ret) != IS_LONG || Z_LVAL(ret) != 0) {
		goto native;
	}
	added = true;
	ZVAL_NULL(&running);
	ZVAL_NEW_REF(&args[1], &running);
	for (;;) {
		zval *r;
		zend_long rc;
		if (!phasync_call_named("curl_multi_exec", &ret, 2, args)) {
			goto cancelled;
		}
		rc = Z_TYPE(ret) == IS_LONG ? Z_LVAL(ret) : 1;
		if (rc != 0 && rc != -1 /* CURLM_CALL_MULTI_PERFORM */) {
			zval_ptr_dtor(&args[1]);
			ZVAL_COPY_VALUE(&args[1], ZEND_CALL_ARG(execute_data, 1));
			goto native;
		}
		r = Z_REFVAL(args[1]);
		if (Z_TYPE_P(r) != IS_LONG || Z_LVAL_P(r) == 0) {
			break;
		}
		{
			zval sel[2];
			ZVAL_COPY_VALUE(&sel[0], &mh);
			ZVAL_DOUBLE(&sel[1], 1.0);
			if (!phasync_call_named("curl_multi_select", &ret, 2, sel)) {
				goto cancelled;
			}
			zval_ptr_dtor(&ret);
		}
	}
	zval_ptr_dtor(&args[1]);
	ZVAL_COPY_VALUE(&args[1], ZEND_CALL_ARG(execute_data, 1));
	if (phasync_call_named("curl_multi_info_read", &info, 1, &mh) && Z_TYPE(info) == IS_ARRAY) {
		zval *res = zend_hash_str_find(Z_ARRVAL(info), "result", sizeof("result") - 1);
		result = res ? zval_get_long(res) : 0;
	}
	zval_ptr_dtor(&info);
	phasync_call_named("curl_multi_remove_handle", &ret, 2, args);
	zval_ptr_dtor(&ret);
	fflush(NULL);                            /* CURLOPT_FILE/WRITEHEADER outputs */
	if (result != 0) {
		zval_ptr_dtor(&mh);
		RETURN_FALSE;
	}
	phasync_call_named("curl_multi_getcontent", &ret, 1, &args[1]);
	zval_ptr_dtor(&mh);
	if (Z_TYPE(ret) == IS_STRING) {
		RETURN_COPY_VALUE(&ret);
	}
	zval_ptr_dtor(&ret);
	RETURN_TRUE;

native:
	zval_ptr_dtor(&ret);
	if (added) {
		phasync_call_named("curl_multi_remove_handle", &ret, 2, args);
		zval_ptr_dtor(&ret);
	}
	zval_ptr_dtor(&mh);
	PHASYNC_G(orig_curl_exec)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
	return;

cancelled:
	zval_ptr_dtor(&args[1]);
	ZVAL_COPY_VALUE(&args[1], ZEND_CALL_ARG(execute_data, 1));
	zend_exception_save();                   /* clean up with the exception set aside */
	phasync_call_named("curl_multi_remove_handle", &ret, 2, args);
	zval_ptr_dtor(&ret);
	zend_exception_restore();
	zval_ptr_dtor(&mh);
}

/* ---- virtualize(): a per-request SAPI -------------------------------------
 *
 * A boundary gives the code it runs, and every fiber started inside it, its own
 * copy of the per-request state PHP otherwise keeps once per process
 * (PHASYNC_VSTATE_FIELDS): the output buffers, the response headers and status,
 * header_register_callback(), the request body behind php://input, the
 * superglobals' arrays (PG(http_globals)), user-abort state and shutdown
 * functions. Global variables, the superglobals' and $_SESSION's symbol table
 * entries among them, stay the process's: a server swaps them itself around
 * resuming a request's fibers if it wants to. The fiber observers
 * swap that state in and out when execution moves between boundaries, so PHP's
 * own functions (echo, ob_*, header(), headers_list(), request_parse_body(),
 * ...) run unchanged. What
 * leaves a boundary goes where a SAPI's would, through the SAPI callbacks, to
 * the $sapi object: ub_write(), send_headers(), flush(), read_post().
 *
 * A fiber belongs to the boundary of the fiber that starts it (the init
 * observer runs in Fiber::start()). The $sapi methods run with the SAPI's own
 * state live, so their output and errors never re-enter the boundary; while one
 * is suspended, its fiber counts as outside. */

typedef struct phasync_boundary {
	uint32_t refcount;            /* the virtualize() call + one per member fiber      */
	bool finalizing;              /* the end sequence is running                       */
	bool ended;                   /* over: stray fibers' output is discarded           */
	zend_fiber_context *owner;    /* the context running the virtualize() call         */
	int exit_status;              /* EG(exit_status) to restore: an exit() here is the */
	                              /* request's, not the worker's                       */
	zval sapi;
	zend_function *m_ub_write, *m_send_headers, *m_flush, *m_read_post, *m_exit,
		*m_connection_aborted;
	zval server_vars;             /* register_server_variables(), until $_SERVER is built */
	php_stream *body_seen;        /* the body stream read_post() last filled...        */
	HashTable *bodies_replaced;   /* ...and earlier ones PHP replaced (each            */
	                              /* request_parse_body() starts a new one), to free   */
	phasync_vstate st;            /* this boundary's state while it is not live        */
#ifdef PHASYNC_HAVE_SESSION
	phasync_vsession session;
#endif
#ifdef PHASYNC_HAVE_FILTER
	phasync_vfilter filter;
#endif
} phasync_boundary;

#define PHASYNC_VKEY(ctx) ((zend_ulong) (uintptr_t) (ctx))

static zend_always_inline phasync_boundary *phasync_v_of(zend_fiber_context *ctx)
{
	return zend_hash_index_find_ptr(&PHASYNC_G(vfibers), PHASYNC_VKEY(ctx));
}

/* Make b's state (NULL: the SAPI's own) the live one. */
static void phasync_v_install(phasync_boundary *b)
{
	phasync_boundary *cur = PHASYNC_G(vb_cur);
	phasync_vstate *st;

	if (b == cur) {
		return;
	}
	st = cur ? &cur->st : &PHASYNC_G(vroot);
#define PHASYNC_VSTATE_SAVE(name, live) st->name = live;
	PHASYNC_VSTATE_FIELDS(PHASYNC_VSTATE_SAVE)
#undef PHASYNC_VSTATE_SAVE
	st = b ? &b->st : &PHASYNC_G(vroot);
#define PHASYNC_VSTATE_LOAD(name, live) live = st->name;
	PHASYNC_VSTATE_FIELDS(PHASYNC_VSTATE_LOAD)
#undef PHASYNC_VSTATE_LOAD
#ifdef PHASYNC_HAVE_FILTER
	if (PHASYNC_G(fg)) {
		zend_filter_globals *fg = PHASYNC_G(fg);
		phasync_vfilter *vf = cur ? &cur->filter : &PHASYNC_G(vroot_filter);
# define PHASYNC_VFILTER_SAVE(field) vf->field = fg->field;
		PHASYNC_VFILTER_FIELDS(PHASYNC_VFILTER_SAVE)
# undef PHASYNC_VFILTER_SAVE
		vf = b ? &b->filter : &PHASYNC_G(vroot_filter);
# define PHASYNC_VFILTER_LOAD(field) fg->field = vf->field;
		PHASYNC_VFILTER_FIELDS(PHASYNC_VFILTER_LOAD)
# undef PHASYNC_VFILTER_LOAD
	}
#endif
#ifdef PHASYNC_HAVE_SESSION
	if (PHASYNC_G(ps)) {
		php_ps_globals *ps = PHASYNC_G(ps);
		phasync_vsession *vs = cur ? &cur->session : &PHASYNC_G(vroot_session);
# define PHASYNC_VSESSION_SAVE(name, field) vs->name = ps->field;
		PHASYNC_VSESSION_FIELDS(PHASYNC_VSESSION_SAVE)
# undef PHASYNC_VSESSION_SAVE
		vs = b ? &b->session : &PHASYNC_G(vroot_session);
# define PHASYNC_VSESSION_LOAD(name, field) ps->field = vs->name;
		PHASYNC_VSESSION_FIELDS(PHASYNC_VSESSION_LOAD)
# undef PHASYNC_VSESSION_LOAD
	}
#endif
	PHASYNC_G(vb_cur) = b;
}

/* A fresh request's state, as sapi_activate() and php_output_activate() leave it. */
static void phasync_vstate_init(phasync_vstate *st)
{
	memset(st, 0, sizeof(*st));
	zend_stack_init(&st->ob_handlers, sizeof(php_output_handler *));
	st->ob_flags = PHP_OUTPUT_ACTIVATED;
	zend_llist_init(&st->headers.headers, sizeof(sapi_header_struct),
		(void (*)(void *)) sapi_free_header, 0);
	st->headers.http_response_code = 200;
	st->headers.send_default_content_type = 1;
#if PHP_VERSION_ID >= 80600
	st->send_header_fcc = empty_fcall_info_cache;
#else
	ZVAL_UNDEF(&st->callback_func);
	st->fci_cache = empty_fcall_info_cache;
#endif
	st->proto_num = 1000;
	st->ignore_user_abort = PG(ignore_user_abort);   /* the ini setting */
	ZVAL_UNDEF(&st->error_handler);
	ZVAL_UNDEF(&st->exception_handler);
	zend_stack_init(&st->error_handlers_mask, sizeof(int));
	zend_stack_init(&st->error_handlers, sizeof(zval));
	zend_stack_init(&st->exception_handlers, sizeof(zval));
}

/* php://input handles point at a body stream without a reference (its
 * php_stream_input_t starts with that pointer): close the ones on this body
 * before freeing it, so one kept past the request fails cleanly as a closed
 * resource instead of reading freed memory. */
static void phasync_v_free_body(php_stream *body)
{
	zend_resource *res;
	int le = php_file_le_stream();

	ZEND_HASH_FOREACH_PTR(&EG(regular_list), res) {
		php_stream *s;
		if (res && res->type == le && (s = res->ptr) && s->abstract && s->ops && s->ops->label
		 && strcmp(s->ops->label, "Input") == 0 && *(php_stream **) s->abstract == body) {
			php_stream_free(s, PHP_STREAM_FREE_CLOSE | PHP_STREAM_FREE_KEEP_RSRC);   /* as fclose() */
		}
	} ZEND_HASH_FOREACH_END();
	php_stream_free(body, PHP_STREAM_FREE_CLOSE);
}

/* Free what a boundary's state owns. It is not live, so this works on the fields. */
static void phasync_vstate_free(phasync_vstate *st)
{
	php_output_handler **h;

	while ((h = zend_stack_top(&st->ob_handlers)) != NULL) {
		php_output_handler_free(h);
		zend_stack_del_top(&st->ob_handlers);
	}
	zend_stack_destroy(&st->ob_handlers);
	if (st->ob_start_file) {
		zend_string_release(st->ob_start_file);
	}
	zend_llist_destroy(&st->headers.headers);
	if (st->headers.mimetype) {
		efree(st->headers.mimetype);
	}
	if (st->headers.http_status_line) {
		efree(st->headers.http_status_line);
	}
#if PHP_VERSION_ID >= 80600
	if (ZEND_FCC_INITIALIZED(st->send_header_fcc)) {
		zend_fcc_dtor(&st->send_header_fcc);
	}
#else
	zval_ptr_dtor(&st->callback_func);
#endif
	if (st->shutdown_functions) {
		zend_hash_destroy(st->shutdown_functions);
		FREE_HASHTABLE(st->shutdown_functions);
	}
	zval_ptr_dtor(&st->error_handler);
	zval_ptr_dtor(&st->exception_handler);
	zend_stack_clean(&st->error_handlers_mask, NULL, 1);
	zend_stack_clean(&st->error_handlers, (void (*)(void *)) ZVAL_PTR_DTOR, 1);
	zend_stack_clean(&st->exception_handlers, (void (*)(void *)) ZVAL_PTR_DTOR, 1);
	if (st->uploaded_files) {           /* deletes the temp files not moved away */
		HashTable *live = SG(rfc1867_uploaded_files);
		SG(rfc1867_uploaded_files) = st->uploaded_files;
		destroy_uploaded_files_hash();
		SG(rfc1867_uploaded_files) = live;
	}
	if (st->request_body) {
		phasync_v_free_body(st->request_body);
	}
	if (st->content_type_dup) {
		efree(st->content_type_dup);
	}
	/* Set from request_info() and read_cookies(): owned copies. */
	if (st->request_method) {
		efree((char *) st->request_method);
	}
	if (st->content_type) {
		efree((char *) st->content_type);
	}
	if (st->query_string) {
		efree(st->query_string);
	}
	if (st->request_uri) {
		efree(st->request_uri);
	}
	if (st->cookie_data) {
		efree(st->cookie_data);
	}
	zval_ptr_dtor(&st->http_post);
	zval_ptr_dtor(&st->http_get);
	zval_ptr_dtor(&st->http_cookie);
	zval_ptr_dtor(&st->http_server);
	zval_ptr_dtor(&st->http_files);
}

/* An extension's globals, looked up rather than linked, so one loaded after
 * this extension works too. NULL if it isn't loaded. */
static void *phasync_module_globals(const char *name, size_t len)
{
	zend_module_entry *m = zend_hash_str_find_ptr(&module_registry, name, len);
	if (m == NULL || !m->module_started) {
		return NULL;
	}
#ifdef ZTS
	return TSRMG_BULK(*m->globals_id_ptr, void *);
#else
	return m->globals_ptr;
#endif
}

#ifdef PHASYNC_HAVE_FILTER
/* ext/filter declares its globals without its module entry knowing them; its INI
 * entry filter.default points at them, as the INI system reaches them. */
static zend_filter_globals *phasync_filter_globals(void)
{
	zend_ini_entry *e = zend_hash_str_find_ptr(EG(ini_directives), ZEND_STRL("filter.default"));
	if (e == NULL) {
		return NULL;
	}
# ifdef ZTS
	return TSRMG_BULK(*(ts_rsrc_id *) e->mh_arg2, zend_filter_globals *);
# else
	return (zend_filter_globals *) e->mh_arg2;
# endif
}
#endif

#ifdef PHASYNC_HAVE_SESSION
static php_ps_globals *phasync_ps_globals(void)
{
	return phasync_module_globals(ZEND_STRL("session"));
}

/* The files save handler locks the session file with flock(2) in C, which would
 * block the worker while another request of the same worker holds the session:
 * a deadlock, as that request can't run to release it. A boundary gets a copy of
 * the handler whose read first waits, cooperatively, until the file can be
 * locked; nothing else runs on PHP's thread between that check and the real
 * lock, so no request of this worker can take it in between. A holder in another
 * process still makes the real flock() wait, as under php-fpm. */
static ps_module phasync_ps_files;
static const ps_module *phasync_ps_files_orig;

/* mod_files' path for a session id (ps_files_path_create()), from session.save_path
 * ("[depth;[mode;]]dir"). */
static bool phasync_ps_files_path(const zend_string *key, char *buf, size_t buflen)
{
# if PHP_VERSION_ID >= 80500
	const char *save_path = PHASYNC_G(ps)->save_path ? ZSTR_VAL(PHASYNC_G(ps)->save_path) : "";
# else
	const char *save_path = PHASYNC_G(ps)->save_path ? PHASYNC_G(ps)->save_path : "";
# endif
	const char *dir = strrchr(save_path, ';');
	size_t depth = dir ? (size_t) ZEND_STRTOL(save_path, NULL, 10) : 0;
	size_t n;

	dir = dir ? dir + 1 : save_path;
	if (*dir == '\0') {
		dir = php_get_temporary_directory();
	}
	n = strlen(dir);
	if (ZSTR_LEN(key) <= depth || buflen < n + 2 * depth + ZSTR_LEN(key) + sizeof("/sess_")) {
		return false;
	}
	memcpy(buf, dir, n);
	buf[n++] = '/';
	for (size_t i = 0; i < depth; i++) {
		buf[n++] = ZSTR_VAL(key)[i];
		buf[n++] = '/';
	}
	memcpy(buf + n, "sess_", 5);
	n += 5;
	memcpy(buf + n, ZSTR_VAL(key), ZSTR_LEN(key) + 1);
	return true;
}

static zend_result phasync_ps_files_read(PS_READ_ARGS)
{
	zval *sleep = phasync_sleep_handler();
	char path[MAXPATHLEN];
	zend_long backoff = 1000;

	if (sleep && EG(active_fiber) && key && phasync_ps_files_path(key, path, sizeof(path))) {
		for (;;) {
			int fd = open(path, O_RDONLY | O_CLOEXEC), rc;
			if (fd < 0) {
				break;                    /* no file yet: nobody holds it */
			}
			rc = flock(fd, LOCK_EX | LOCK_NB);
			close(fd);                    /* releases the probe's lock */
			if (rc == 0 || errno != EWOULDBLOCK) {
				break;
			}
			if (phasync_call_sleep(sleep, backoff) < 0) {
				return FAILURE;           /* cancelled: the exception propagates */
			}
			backoff = MIN(backoff * 2, 50000);
		}
	}
	return phasync_ps_files_orig->s_read(mod_data, key, val, maxlifetime);
}

/* A fresh request's session state, as ext/session's RINIT leaves it: no session,
 * the worker's save handler (from session.save_handler at its request start). */
static void phasync_vsession_init(phasync_vsession *vs)
{
	php_ps_globals *ps = PHASYNC_G(ps);
	zval *names = (zval *) &vs->user_names;

	memset(vs, 0, sizeof(*vs));
	vs->mod = ps->mod;
	if (vs->mod && strcmp(vs->mod->s_name, "files") == 0) {
		if (!phasync_ps_files_orig) {
			phasync_ps_files_orig = vs->mod;
			phasync_ps_files = *vs->mod;
			phasync_ps_files.s_read = phasync_ps_files_read;
		}
		if (vs->mod == phasync_ps_files_orig) {
			vs->mod = &phasync_ps_files;
		}
	}
	vs->status = ps->mod ? php_session_none : php_session_disabled;
	vs->define_sid = 1;
	ZVAL_UNDEF(&vs->http_vars);
	/* The worker's user save handler (session_set_save_handler() outside any
	 * boundary), as ps->mod already is: the request holds its own references. */
	{
		zval *worker = (zval *) &ps->mod_user_names;
		for (size_t i = 0; i < sizeof(vs->user_names) / sizeof(zval); i++) {
			ZVAL_COPY(&names[i], &worker[i]);
		}
	}
	vs->user_implemented = ps->mod_user_implemented;
	if (ps->mod_user_class_name) {
		vs->user_class_name = zend_string_copy(ps->mod_user_class_name);
	}
# if PHP_VERSION_ID >= 80600
	vs->user_obj_methods = ps->mod_user_uses_object_methods_as_handlers;
# endif
}

/* Free what a boundary's session state owns (it is not live). */
static void phasync_vsession_free(phasync_vsession *vs)
{
	zval *names = (zval *) &vs->user_names;

	zval_ptr_dtor(&vs->http_vars);
	if (vs->id) {
		zend_string_release(vs->id);
	}
	if (vs->vars) {
		zend_string_release(vs->vars);
	}
	if (vs->user_class_name) {
		zend_string_release(vs->user_class_name);
	}
# if PHP_VERSION_ID >= 80300
	if (vs->started_file) {
		zend_string_release(vs->started_file);
	}
# endif
	for (size_t i = 0; i < sizeof(vs->user_names) / sizeof(zval); i++) {
		zval_ptr_dtor(&names[i]);
	}
}

/* The session's end of request, as ext/session's RSHUTDOWN does it: write and
 * close an active session, then close the save handler (releasing a files
 * session's lock). Runs with the boundary live. */
static void phasync_vsession_end(void)
{
	php_ps_globals *ps = PHASYNC_G(ps);

	if (ps->session_status == php_session_active) {
		zend_try {
			php_session_flush(1);
		} zend_end_try();
	}
	zval_ptr_dtor(&ps->http_session_vars);
	ZVAL_UNDEF(&ps->http_session_vars);
	if (ps->mod && (ps->mod_data || ps->mod_user_implemented)) {
		zend_try {
			ps->mod->s_close(&ps->mod_data);
		} zend_end_try();
	}
	ps->mod_data = NULL;
	ps->session_status = php_session_none;
}
#endif

static void phasync_v_release(phasync_boundary *b)
{
	if (--b->refcount == 0) {
		if (b->bodies_replaced) {
			php_stream *body;
			ZEND_HASH_FOREACH_PTR(b->bodies_replaced, body) {
				phasync_v_free_body(body);
			} ZEND_HASH_FOREACH_END();
			zend_hash_destroy(b->bodies_replaced);
			FREE_HASHTABLE(b->bodies_replaced);
		}
		phasync_vstate_free(&b->st);
		zval_ptr_dtor(&b->server_vars);
#ifdef PHASYNC_HAVE_FILTER
		if (PHASYNC_G(fg)) {
# define PHASYNC_VFILTER_FREE(field) zval_ptr_dtor(&b->filter.field);
			PHASYNC_VFILTER_FIELDS(PHASYNC_VFILTER_FREE)
# undef PHASYNC_VFILTER_FREE
		}
#endif
#ifdef PHASYNC_HAVE_SESSION
		if (PHASYNC_G(ps)) {
			phasync_vsession_free(&b->session);
		}
#endif
		zval_ptr_dtor(&b->sapi);
		efree(b);
		PHASYNC_G(vcount)--;
	}
}

/* Put back an exception set aside around a call: the call's own exception is
 * chained before it, unless it is exit()'s or the unwind of a coroutine being
 * destroyed, which go on, and what failed on their way is lost (a SAPI's output
 * errors never throw). */
static void phasync_v_restore_exception(zend_object *ex)
{
	if (ex == NULL) {
		return;
	}
	if (EG(exception) && !zend_is_unwind_exit(ex) && !zend_is_graceful_exit(ex)) {
		zend_exception_set_previous(EG(exception), ex);
		return;
	}
	if (EG(exception)) {
		OBJ_RELEASE(EG(exception));
	}
	EG(exception) = ex;
}

/* Call a $sapi method with the SAPI's own state live. A pending exception (output
 * during unwinding) is set aside for the call and put back after it. */
static void phasync_v_call(phasync_boundary *b, zend_function *fn, zval *rv, uint32_t argc, zval *argv)
{
	zend_ulong key = PHASYNC_VKEY(EG(current_fiber_context));
	zend_object *ex = EG(exception);
	bool member = zend_hash_index_del(&PHASYNC_G(vfibers), key) == SUCCESS;

	EG(exception) = NULL;
	phasync_v_install(NULL);
	ZVAL_UNDEF(rv);
	zend_call_known_instance_method(fn, Z_OBJ(b->sapi), rv, argc, argv);
	if (member) {
		zend_hash_index_add_new_ptr(&PHASYNC_G(vfibers), key, b);
		phasync_v_install(b);
	}
	phasync_v_restore_exception(ex);
}

/* ---- fiber observers ---- */

/* Settle what a ledger still holds: its coroutine is gone without its code after
 * the suspension having run (see the ledger). Its slots are taken back first. */
static void phasync_ledger_settle(phasync_ledger *l)
{
	if (l->task) {
		phasync_task_finish(l->task, l->task_p, NULL);
		l->task = NULL;
	}
	if (l->wait_p) {
		phasync_chan_purge(l->wait_p->chan, l->wait_slot);
		phasync_wait_settle(l->wait_p, l->wait_idx, l->wait_stream, l->wait_fd, l->wait_slot, l->wait_dupfd);
		l->wait_p = NULL;
	}
	if (l->inflight) {
		phasync_inflight_out(l->inflight);
		l->inflight = NULL;
	}
	if (l->closing) {
		phasync_inflight *f = zend_hash_index_find_ptr(&PHASYNC_G(inflight), (zend_ulong) (uintptr_t) l->closing);
		if (f && f->closer == l->closing_p) {
			phasync_chan_purge(f->closer->chan, f->closer_slot);
			f->closer = NULL;
			if (f->hold) {
				phasync_hold_drop(f);
			}
		}
		OBJ_RELEASE(&l->closing_p->std);
		l->closing = NULL;
	}
}

static void phasync_ledger_close(zend_fiber_context *ctx)
{
	phasync_ledger *l = zend_hash_index_find_ptr(&PHASYNC_G(ledgers), (zend_ulong) (uintptr_t) ctx);

	if (l) {
		phasync_ledger_settle(l);
		zend_hash_index_del(&PHASYNC_G(ledgers), (zend_ulong) (uintptr_t) ctx);
	}
}

static void phasync_v_fiber_init(zend_fiber_context *ctx)
{
	phasync_boundary *b;
	if (PHASYNC_G(vcount) && (b = phasync_v_of(EG(current_fiber_context)))) {
		zend_hash_index_add_new_ptr(&PHASYNC_G(vfibers), PHASYNC_VKEY(ctx), b);
		b->refcount++;
	}
}

static void phasync_v_fiber_switch(zend_fiber_context *from, zend_fiber_context *to)
{
	if (PHASYNC_G(vcount)) {
		phasync_v_install(phasync_v_of(to));
		/* A fiber that ended by exit() (phasync_v_before_exit()) leaves no result;
		 * make getReturn() give null, as for a fiber that returned nothing. */
		if (from->status == ZEND_FIBER_STATUS_DEAD && from->kind == zend_ce_fiber) {
			zend_fiber *f = (zend_fiber *) ((char *) from - XtOffsetOf(zend_fiber, context));
			if ((f->flags & ZEND_FIBER_FLAG_DESTROYED) && Z_ISUNDEF(f->result)) {
				ZVAL_NULL(&f->result);
			}
		}
	}
}

static void phasync_v_fiber_destroy(zend_fiber_context *ctx)
{
	phasync_boundary *b;

	phasync_ledger_close(ctx);
	if (PHASYNC_G(vcount) && (b = phasync_v_of(ctx))) {
		zend_hash_index_del(&PHASYNC_G(vfibers), PHASYNC_VKEY(ctx));
		phasync_v_release(b);
	}
}

/* ---- SAPI callbacks, routed to the live boundary's $sapi ---- */

static size_t (*phasync_v_ub_write_prev)(const char *str, size_t len);
static int (*phasync_v_send_headers_prev)(sapi_headers_struct *h);
static void (*phasync_v_flush_prev)(void *server_context);
static size_t (*phasync_v_read_post_prev)(char *buf, size_t len);
static int (*phasync_v_header_handler_prev)(sapi_header_struct *h, sapi_header_op_enum op, sapi_headers_struct *hs);
static void (*phasync_v_register_server_variables_prev)(zval *track_vars_array);

static void phasync_v_before_exit(zval *status);

/* The client is gone: what php_handle_aborted_connection() does, except that
 * ending the request ends only the boundary (through exit()) and not the worker. */
static void phasync_v_aborted(void)
{
	PG(connection_status) = PHP_CONNECTION_ABORTED;
	php_output_set_status(PHP_OUTPUT_DISABLED);
	if (!PG(ignore_user_abort) && !EG(exception)) {
		phasync_v_before_exit(NULL);
		zend_throw_unwind_exit();
	}
}

static size_t phasync_v_ub_write(const char *str, size_t len)
{
	phasync_boundary *b = PHASYNC_G(vb_cur);
	zval arg, rv;

	if (!b) {
		return phasync_v_ub_write_prev(str, len);
	}
	if (b->ended) {
		return len;
	}
	ZVAL_STRINGL(&arg, str, len);
	phasync_v_call(b, b->m_ub_write, &rv, 1, &arg);
	zval_ptr_dtor(&arg);
	if (Z_TYPE(rv) == IS_FALSE) {
		phasync_v_aborted();
	}
	zval_ptr_dtor(&rv);
	return len;
}

static int phasync_v_send_headers(sapi_headers_struct *h)
{
	phasync_boundary *b = PHASYNC_G(vb_cur);
	zend_llist_position pos;
	sapi_header_struct *hdr;
	zval args[3], rv;

	if (!b) {
		return phasync_v_send_headers_prev ? phasync_v_send_headers_prev(h) : SAPI_HEADER_DO_SEND;
	}
	if (b->ended) {
		return SAPI_HEADER_SENT_SUCCESSFULLY;
	}
	ZVAL_LONG(&args[0], h->http_response_code);
	if (h->http_status_line) {
		ZVAL_STRING(&args[1], h->http_status_line);
	} else {
		ZVAL_NULL(&args[1]);
	}
	array_init_size(&args[2], zend_llist_count(&h->headers));
	for (hdr = zend_llist_get_first_ex(&h->headers, &pos); hdr; hdr = zend_llist_get_next_ex(&h->headers, &pos)) {
		add_next_index_stringl(&args[2], hdr->header, hdr->header_len);
	}
	phasync_v_call(b, b->m_send_headers, &rv, 3, args);
	zval_ptr_dtor(&args[1]);
	zval_ptr_dtor(&args[2]);
	zval_ptr_dtor(&rv);
	return SAPI_HEADER_SENT_SUCCESSFULLY;
}

static void phasync_v_flush(void *server_context)
{
	phasync_boundary *b = PHASYNC_G(vb_cur);
	zval rv;

	if (!b) {
		if (phasync_v_flush_prev) {
			phasync_v_flush_prev(server_context);
		}
		return;
	}
	if (b->m_flush && !b->ended) {
		phasync_v_call(b, b->m_flush, &rv, 0, NULL);
		zval_ptr_dtor(&rv);
	}
}

/* Like a SAPI's read_post: up to len bytes, fewer only at the end of the body. */
static size_t phasync_v_read_post(char *buf, size_t len)
{
	phasync_boundary *b = PHASYNC_G(vb_cur);
	size_t n = 0;
	zval arg, rv;

	if (!b) {
		return phasync_v_read_post_prev ? phasync_v_read_post_prev(buf, len) : 0;
	}
	if (b->body_seen != SG(request_info).request_body) {
		if (b->body_seen) {
			if (!b->bodies_replaced) {
				ALLOC_HASHTABLE(b->bodies_replaced);
				zend_hash_init(b->bodies_replaced, 2, NULL, NULL, 0);
			}
			zend_hash_next_index_insert_ptr(b->bodies_replaced, b->body_seen);
		}
		b->body_seen = SG(request_info).request_body;
	}
	if (!b->m_read_post || b->ended) {
		return 0;
	}
	ZVAL_LONG(&arg, (zend_long) len);
	phasync_v_call(b, b->m_read_post, &rv, 1, &arg);
	if (Z_TYPE(rv) == IS_STRING) {
		n = MIN(len, Z_STRLEN(rv));
		memcpy(buf, Z_STRVAL(rv), n);
	}
	zval_ptr_dtor(&rv);
	return n;
}

/* $_SERVER's entries from register_server_variables(), registered as a SAPI
 * registers its own: through the input filter, then php_register_variable_safe(). */
static void phasync_v_register_server_variables(zval *track_vars_array)
{
	phasync_boundary *b = PHASYNC_G(vb_cur);
	zend_string *key;
	zval *v;

	if (!b) {
		if (phasync_v_register_server_variables_prev) {
			phasync_v_register_server_variables_prev(track_vars_array);
		}
		return;
	}
	if (Z_TYPE(b->server_vars) != IS_ARRAY) {
		return;
	}
	ZEND_HASH_FOREACH_STR_KEY_VAL(Z_ARRVAL(b->server_vars), key, v) {
		zend_string *str;
		char *val;
		size_t new_len;

		if (!key) {
			continue;
		}
		str = zval_get_string(v);
		val = ZSTR_VAL(str);
		if (sapi_module.input_filter(PARSE_SERVER, ZSTR_VAL(key), &val, ZSTR_LEN(str), &new_len)) {
			php_register_variable_safe(ZSTR_VAL(key), val, new_len, track_vars_array);
		}
		zend_string_release(str);
	} ZEND_HASH_FOREACH_END();
}

/* Installed only if the SAPI has one: headers set in a boundary are the
 * boundary's, so the SAPI's own handler doesn't see them. */
static int phasync_v_header_handler(sapi_header_struct *h, sapi_header_op_enum op, sapi_headers_struct *hs)
{
	return PHASYNC_G(vb_cur) ? SAPI_HEADER_ADD : phasync_v_header_handler_prev(h, op, hs);
}

/* ---- exit(), die(), connection_aborted(), connection_status() ---- */

/* An exit() inside a boundary ends the request, not the worker. PHP unwinds it as
 * an uncatchable exception; in the fiber running virtualize(), virtualize()
 * stops it. In a fiber started inside the boundary it would reach whoever resumed
 * that fiber (the event loop), so the fiber is marked as being destroyed, which
 * makes it end quietly (zend_fiber_execute() drops an exit unwinding a destroyed
 * fiber), and $sapi->exit() is told, so the server can cancel the request. */
static void phasync_v_before_exit(zval *status)
{
	phasync_boundary *b = PHASYNC_G(vb_cur);
	zval arg, rv;

	if (!b || b->finalizing || b->ended) {
		return;
	}
	if (b->m_exit) {
		if (status && (Z_TYPE_P(status) == IS_STRING || Z_TYPE_P(status) == IS_LONG)) {
			ZVAL_COPY(&arg, status);
		} else {
			ZVAL_LONG(&arg, 0);
		}
		phasync_v_call(b, b->m_exit, &rv, 1, &arg);
		zval_ptr_dtor(&arg);
		zval_ptr_dtor(&rv);
	}
	if (EG(current_fiber_context) != b->owner && EG(active_fiber)) {
		EG(active_fiber)->flags |= ZEND_FIBER_FLAG_DESTROYED;
	}
}

#if PHP_VERSION_ID >= 80400
static ZEND_NAMED_FUNCTION(phasync_exit_override)
{
	if (PHASYNC_G(vb_cur)) {
		phasync_v_before_exit(ZEND_CALL_NUM_ARGS(execute_data) ? ZEND_CALL_ARG(execute_data, 1) : NULL);
	}
	PHASYNC_G(orig_exit)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}
#else
/* Before 8.4, exit is the ZEND_EXIT opcode. */
static user_opcode_handler_t phasync_exit_opcode_prev;

static int phasync_exit_opcode(zend_execute_data *execute_data)
{
	if (PHASYNC_G(vb_cur)) {
		const zend_op *opline = EX(opline);
		phasync_v_before_exit(opline->op1_type == IS_UNUSED ? NULL
			: zend_get_zval_ptr(opline, opline->op1_type, &opline->op1, execute_data));
	}
	return phasync_exit_opcode_prev ? phasync_exit_opcode_prev(execute_data) : ZEND_USER_OPCODE_DISPATCH;
}
#endif

/* A server may know the client left before any write fails. */
static void phasync_v_ask_aborted(void)
{
	phasync_boundary *b = PHASYNC_G(vb_cur);
	zval rv;

	if (b && b->m_connection_aborted && !b->ended && !(PG(connection_status) & PHP_CONNECTION_ABORTED)) {
		phasync_v_call(b, b->m_connection_aborted, &rv, 0, NULL);
		if (zend_is_true(&rv)) {
			PG(connection_status) |= PHP_CONNECTION_ABORTED;
		}
		zval_ptr_dtor(&rv);
	}
}

static ZEND_NAMED_FUNCTION(phasync_connection_aborted_override)
{
	phasync_v_ask_aborted();
	PHASYNC_G(orig_connection_aborted)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static ZEND_NAMED_FUNCTION(phasync_connection_status_override)
{
	phasync_v_ask_aborted();
	PHASYNC_G(orig_connection_status)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

/* ---- virtualize() ---- */

/* request_info(): ['method' => ..., 'content_type' => ..., 'content_length' => ...,
 * 'query_string' => ..., 'request_uri' => ...] describes the request, as a SAPI's
 * request_info does. */
static void phasync_v_request_info(phasync_boundary *b, zend_function *fn)
{
	zval rv, *v;

	phasync_v_call(b, fn, &rv, 0, NULL);
	if (Z_TYPE(rv) == IS_ARRAY) {
		if ((v = zend_hash_str_find_deref(Z_ARRVAL(rv), ZEND_STRL("method"))) && Z_TYPE_P(v) == IS_STRING) {
			b->st.request_method = estrndup(Z_STRVAL_P(v), Z_STRLEN_P(v));
			b->st.headers_only = strcasecmp(b->st.request_method, "HEAD") == 0;
		}
		if ((v = zend_hash_str_find_deref(Z_ARRVAL(rv), ZEND_STRL("content_type"))) && Z_TYPE_P(v) == IS_STRING) {
			b->st.content_type = estrndup(Z_STRVAL_P(v), Z_STRLEN_P(v));
		}
		if ((v = zend_hash_str_find_deref(Z_ARRVAL(rv), ZEND_STRL("content_length"))) && Z_TYPE_P(v) == IS_LONG) {
			b->st.content_length = Z_LVAL_P(v);
		}
		if ((v = zend_hash_str_find_deref(Z_ARRVAL(rv), ZEND_STRL("query_string"))) && Z_TYPE_P(v) == IS_STRING) {
			b->st.query_string = estrndup(Z_STRVAL_P(v), Z_STRLEN_P(v));
		}
		if ((v = zend_hash_str_find_deref(Z_ARRVAL(rv), ZEND_STRL("request_uri"))) && Z_TYPE_P(v) == IS_STRING) {
			b->st.request_uri = estrndup(Z_STRVAL_P(v), Z_STRLEN_P(v));
		}
	}
	zval_ptr_dtor(&rv);
}

/* What sapi_activate() asks the SAPI for at the start of a request, besides
 * request_info(): the Cookie header and the server variables. Called with the
 * worker's state live, before the superglobals are built, so building them runs
 * no PHP code of the server's but read_post() for a form body. */
static void phasync_v_request_start(phasync_boundary *b, zend_function *m_cookies, zend_function *m_server)
{
	zval rv;

	if (m_cookies && !EG(exception)) {
		phasync_v_call(b, m_cookies, &rv, 0, NULL);
		if (Z_TYPE(rv) == IS_STRING) {
			b->st.cookie_data = estrndup(Z_STRVAL(rv), Z_STRLEN(rv));
		}
		zval_ptr_dtor(&rv);
	}
	if (m_server && !EG(exception)) {
		phasync_v_call(b, m_server, &b->server_vars, 0, NULL);
	}
}

/* The request's form body, as sapi_activate() reads it through
 * sapi_read_post_data() for a POST: the content type's post reader (for
 * application/x-www-form-urlencoded, the whole body into the stream behind
 * php://input) and the handler that sapi_handle_post() will run for $_POST. A
 * content type PHP has no handler for isn't read ahead ("swallowed") here:
 * php://input reads it from read_post() when the code asks. */
static void phasync_v_read_post_data(void)
{
	const char *ct = SG(request_info).content_type;
	size_t n = strcspn(ct, ";, ");
	char *type = zend_str_tolower_dup(ct, n);
	sapi_post_entry *entry = zend_hash_str_find_ptr(&SG(known_post_content_types), type, n);

	efree(type);
	if (!entry) {
		return;
	}
	SG(request_info).post_entry = entry;
	SG(request_info).content_type_dup = estrdup(ct);
	zend_str_tolower(SG(request_info).content_type_dup, n);
	if (entry->post_reader) {
		entry->post_reader();
	}
}

/* The request's superglobals, as PHP builds them at the start of a request
 * (sapi_activate(), php_hash_environment()): the form body read, then each
 * auto global's own callback, which parses through sapi_module.treat_data (the
 * query string, the Cookie header, the body by its post handler, with ext/filter
 * seeing each variable) and registers the result. Runs with the boundary live.
 *
 * All are built at the start, including those PHP builds just in time ($_SERVER,
 * $_REQUEST): PHP arms those per request and builds them when a script using them
 * is compiled or loaded from opcache, which in a worker happened long before this
 * request. $_POST and $_FILES come from the body at the start as well, as PHP
 * does: nothing can build them later, since code reads the arrays directly. */
static void phasync_v_build_superglobals(void)
{
	if (PG(enable_post_data_reading) && SG(request_info).content_type
	 && SG(request_info).request_method && strcmp(SG(request_info).request_method, "POST") == 0) {
		phasync_v_read_post_data();
	}
	for (int i = 0; i < PHASYNC_NSG && !EG(exception); i++) {
		zend_auto_global *ag = zend_hash_find_ptr(CG(auto_globals), phasync_sg_names[i]);
		if (i == PHASYNC_SG_SERVER) {
			/* A web request's $_SERVER has no argv or argc: the worker's CLI
			 * arguments aren't the request's, and deriving argv from the query
			 * string (register_argc_argv, which the CLI forces on) is deprecated. */
			int argc = SG(request_info).argc;
			bool reg = PG(register_argc_argv);
			SG(request_info).argc = 0;
			PG(register_argc_argv) = 0;
			ag->auto_global_callback(ag->name);
			SG(request_info).argc = argc;
			PG(register_argc_argv) = reg;
		} else {
			ag->auto_global_callback(ag->name);
		}
	}
}

/* The end of a request, in php_request_shutdown()'s order: shutdown functions,
 * the output buffers flushed (removable or not), the headers if no output sent
 * them, then the session written and closed. Runs with the boundary live, in the
 * virtualize() call's fiber. */
static void phasync_v_finalize(phasync_boundary *b)
{
	zend_object *ex = EG(exception);

	EG(exception) = NULL;
	b->finalizing = true;
	php_call_shutdown_functions();
	if (EG(exception) && zend_is_unwind_exit(EG(exception))) {
		zend_clear_exception();       /* exit() in a shutdown function */
	}
	php_free_shutdown_functions();
	if (!EG(exception)) {
		php_output_end_all();
	}
	if (!EG(exception) && !SG(headers_sent)) {
		sapi_send_headers();
	}
#ifdef PHASYNC_HAVE_SESSION
	if (PHASYNC_G(ps)) {
		phasync_vsession_end();
	}
#endif
	php_output_set_status(PHP_OUTPUT_DISABLED);
	b->ended = true;
	EG(exit_status) = b->exit_status;
	phasync_v_restore_exception(ex);
}

ZEND_FUNCTION(phasync_ext_virtualize)
{
	zval *code, *sapi, retval;
	zend_class_entry *ce;
	phasync_boundary *b;
	zend_fiber_context *ctx = EG(current_fiber_context);
	zend_function *m_request_info, *m_read_cookies, *m_register_server_variables;
	bool bailout = false;

	ZEND_PARSE_PARAMETERS_START(2, 2)
		Z_PARAM_OBJECT_OF_CLASS(code, zend_ce_closure)
		Z_PARAM_OBJECT(sapi)
	ZEND_PARSE_PARAMETERS_END();

	if (PHASYNC_G(vb_cur)) {
		zend_throw_error(NULL, "phasync\\ext\\virtualize() cannot be nested");
		RETURN_THROWS();
	}
	ce = Z_OBJCE_P(sapi);
	b = ecalloc(1, sizeof(*b));
#define PHASYNC_V_METHOD(name) zend_hash_str_find_ptr_lc(&ce->function_table, ZEND_STRL(name))
	b->m_ub_write = PHASYNC_V_METHOD("ub_write");
	b->m_send_headers = PHASYNC_V_METHOD("send_headers");
	b->m_flush = PHASYNC_V_METHOD("flush");
	b->m_read_post = PHASYNC_V_METHOD("read_post");
	b->m_exit = PHASYNC_V_METHOD("exit");
	b->m_connection_aborted = PHASYNC_V_METHOD("connection_aborted");
	m_request_info = PHASYNC_V_METHOD("request_info");
	m_read_cookies = PHASYNC_V_METHOD("read_cookies");
	m_register_server_variables = PHASYNC_V_METHOD("register_server_variables");
#undef PHASYNC_V_METHOD
	if (!b->m_ub_write || !b->m_send_headers) {
		efree(b);
		zend_argument_value_error(2, "must have the methods ub_write() and send_headers()");
		RETURN_THROWS();
	}
	b->refcount = 2;                  /* this call + the calling fiber's membership */
	b->owner = ctx;
	b->exit_status = EG(exit_status);
	ZVAL_COPY(&b->sapi, sapi);
	phasync_vstate_init(&b->st);
	{
		struct timeval tp;           /* REQUEST_TIME: now, as sapi_get_request_time() */
		gettimeofday(&tp, NULL);
		b->st.request_time = (double) tp.tv_sec + tp.tv_usec / 1000000.00;
	}
#ifdef PHASYNC_HAVE_SESSION
	if (!PHASYNC_G(vcount)) {
		PHASYNC_G(ps) = phasync_ps_globals();
	}
	if (PHASYNC_G(ps)) {
		phasync_vsession_init(&b->session);
	}
#endif
	if (!PHASYNC_G(vcount)) {
#ifdef PHASYNC_HAVE_FILTER
		PHASYNC_G(fg) = phasync_filter_globals();
#endif
		/* The worker's own superglobals PHP builds just in time: build them now,
		 * as a script using them would, so that isn't first done in a boundary. */
		for (int i = 0; i < PHASYNC_NSG; i++) {
			zend_is_auto_global(phasync_sg_names[i]);
		}
	}
	PHASYNC_G(vcount)++;
	if (m_request_info) {
		phasync_v_request_info(b, m_request_info);
	}
	phasync_v_request_start(b, m_read_cookies, m_register_server_variables);
	zend_hash_index_add_new_ptr(&PHASYNC_G(vfibers), PHASYNC_VKEY(ctx), b);

	ZVAL_UNDEF(&retval);
	if (!EG(exception)) {
		phasync_v_install(b);
		zend_try {
			phasync_v_build_superglobals();
			zval_ptr_dtor(&b->server_vars);
			ZVAL_UNDEF(&b->server_vars);
			if (!EG(exception)) {
				call_user_function(NULL, NULL, code, &retval, 0, NULL);
			}
			if (EG(exception) && zend_is_unwind_exit(EG(exception))) {
				zend_clear_exception();   /* exit(): the request ends here */
			} else if (EG(exception) && !zend_is_graceful_exit(EG(exception))
			        && Z_TYPE(EG(user_exception_handler)) != IS_UNDEF) {
				zend_user_exception_handler();   /* uncaught, as at the top of a script */
			}
			phasync_v_finalize(b);
		} zend_catch {
			bailout = true;
		} zend_end_try();
	}

	zend_hash_index_del(&PHASYNC_G(vfibers), PHASYNC_VKEY(ctx));
	phasync_v_install(NULL);
	b->ended = true;
	phasync_v_release(b);             /* membership */
	phasync_v_release(b);             /* this call */
	if (bailout) {
		zend_bailout();
	}
	if (EG(exception)) {
		zval_ptr_dtor(&retval);
		RETURN_THROWS();
	}
	if (Z_TYPE(retval) == IS_UNDEF) {
		RETURN_NULL();
	}
	RETURN_COPY_VALUE(&retval);
}

/* fclose() in a coroutine being destroyed (its finally blocks), of a stream other
 * coroutines are inside an op on: it cannot wait for them, and its close op would
 * free the stream under them. Close it as far as they are concerned (woken, their
 * ops fail with EBADF) and leave the stream to its last reference, which they hold. */
static ZEND_NAMED_FUNCTION(phasync_fclose_override)
{
	zval *res;
	php_stream *stream;
	phasync_inflight *f;

	if (phasync_unwinding() && ZEND_NUM_ARGS() == 1
	 && Z_TYPE_P(res = ZEND_CALL_ARG(execute_data, 1)) == IS_RESOURCE
	 && (stream = zend_fetch_resource2(Z_RES_P(res), NULL, php_file_le_stream(), php_file_le_pstream()))
	 && !(stream->flags & PHP_STREAM_FLAG_NO_FCLOSE)
	 && (f = zend_hash_index_find_ptr(&PHASYNC_G(inflight), (zend_ulong) (uintptr_t) stream))
	 && f->waiters) {
		phasync_stream_forget(stream);
		f->closing = true;
		RETURN_TRUE;
	}
	PHASYNC_G(orig_fclose)(INTERNAL_FUNCTION_PARAM_PASSTHRU);
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
	PHASYNC_G(orig_udp) = zend_hash_str_find_ptr(xhash, "udp", sizeof("udp") - 1);
	PHASYNC_G(orig_udg) = zend_hash_str_find_ptr(xhash, "udg", sizeof("udg") - 1);
	if (PHASYNC_G(orig_udp)) php_stream_xport_register("udp", phasync_udp_factory);
	if (PHASYNC_G(orig_udg)) php_stream_xport_register("udg", phasync_udg_factory);

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
	if ((f = phasync_find_ifunc("popen", sizeof("popen") - 1))) {
		PHASYNC_G(orig_popen) = f->handler;
		f->handler = phasync_popen_override;
	}
	if ((f = phasync_find_ifunc("exec", sizeof("exec") - 1))) {
		PHASYNC_G(orig_exec) = f->handler;
		f->handler = phasync_exec_override;
	}
	if ((f = phasync_find_ifunc("system", sizeof("system") - 1))) {
		PHASYNC_G(orig_system) = f->handler;
		f->handler = phasync_system_override;
	}
	if ((f = phasync_find_ifunc("passthru", sizeof("passthru") - 1))) {
		PHASYNC_G(orig_passthru) = f->handler;
		f->handler = phasync_passthru_override;
	}
	if ((f = phasync_find_ifunc("shell_exec", sizeof("shell_exec") - 1))) {
		PHASYNC_G(orig_shell_exec) = f->handler;
		f->handler = phasync_shell_exec_override;
	}
	if ((f = phasync_find_ifunc("proc_close", sizeof("proc_close") - 1))) {
		PHASYNC_G(orig_proc_close) = f->handler;
		f->handler = phasync_proc_close_override;
	}
	if ((f = phasync_find_ifunc("curl_exec", sizeof("curl_exec") - 1))) {
		PHASYNC_G(orig_curl_exec) = f->handler;
		f->handler = phasync_curl_exec_override;
	}
	if ((f = phasync_find_ifunc("sem_acquire", sizeof("sem_acquire") - 1))) {
		PHASYNC_G(orig_sem_acquire) = f->handler;
		f->handler = phasync_sem_acquire_override;
	}
	if ((f = phasync_find_ifunc("msg_receive", sizeof("msg_receive") - 1))) {
		PHASYNC_G(orig_msg_receive) = f->handler;
		f->handler = phasync_msg_receive_override;
	}
	if ((f = phasync_find_ifunc("curl_multi_select", sizeof("curl_multi_select") - 1))) {
		PHASYNC_G(orig_curl_multi_select) = f->handler;
		f->handler = phasync_curl_multi_select_override;
	}
	if ((f = phasync_find_ifunc("socket_connect", sizeof("socket_connect") - 1))) {
		PHASYNC_G(orig_socket_connect) = f->handler;
		f->handler = phasync_socket_connect_override;
	}
	if ((f = phasync_find_ifunc("socket_close", sizeof("socket_close") - 1))) {
		PHASYNC_G(orig_socket_close) = f->handler;
		f->handler = phasync_socket_close_override;
	}
	for (zend_long i = 0; i < (zend_long) PHASYNC_SOCK_NFUNCS; i++) {
		if ((f = phasync_find_ifunc(phasync_sock_funcs[i].name, strlen(phasync_sock_funcs[i].name)))) {
			zval zi;
			ZVAL_LONG(&zi, i);
			PHASYNC_G(orig_sock)[i] = f->handler;
			f->handler = phasync_socket_io_override;
			zend_hash_index_update(&PHASYNC_G(sock_hooks), (zend_ulong) (uintptr_t) f->function_name, &zi);
		}
	}
	if ((f = phasync_find_ifunc("pcntl_waitpid", sizeof("pcntl_waitpid") - 1))) {
		PHASYNC_G(orig_pcntl_waitpid) = f->handler;
		f->handler = phasync_pcntl_waitpid_override;
	}
	if ((f = phasync_find_ifunc("pcntl_wait", sizeof("pcntl_wait") - 1))) {
		PHASYNC_G(orig_pcntl_wait) = f->handler;
		f->handler = phasync_pcntl_wait_override;
	}
	for (zend_long i = 0; i < (zend_long) PHASYNC_FS_NFUNCS; i++) {
		if ((f = phasync_find_ifunc(phasync_fs_funcs[i].name, strlen(phasync_fs_funcs[i].name)))) {
			zval zi;
			ZVAL_LONG(&zi, i);
			PHASYNC_G(orig_fs)[i] = f->handler;
			f->handler = phasync_fs_override;
			zend_hash_index_update(&PHASYNC_G(fs_hooks), (zend_ulong) (uintptr_t) f->function_name, &zi);
		}
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
	if ((f = phasync_find_ifunc("fclose", sizeof("fclose") - 1))) {
		PHASYNC_G(orig_fclose) = f->handler;
		f->handler = phasync_fclose_override;
	}
#if PHP_VERSION_ID >= 80400
	/* die is an alias: its own entry, the same handler. */
	if ((f = phasync_find_ifunc("exit", sizeof("exit") - 1))) {
		PHASYNC_G(orig_exit) = f->handler;
		f->handler = phasync_exit_override;
	}
	if ((f = phasync_find_ifunc("die", sizeof("die") - 1)) && f->handler == PHASYNC_G(orig_exit)) {
		f->handler = phasync_exit_override;
	}
#endif
	if ((f = phasync_find_ifunc("connection_aborted", sizeof("connection_aborted") - 1))) {
		PHASYNC_G(orig_connection_aborted) = f->handler;
		f->handler = phasync_connection_aborted_override;
	}
	if ((f = phasync_find_ifunc("connection_status", sizeof("connection_status") - 1))) {
		PHASYNC_G(orig_connection_status) = f->handler;
		f->handler = phasync_connection_status_override;
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
	if (PHASYNC_G(orig_udp))  php_stream_xport_register("udp", PHASYNC_G(orig_udp));
	if (PHASYNC_G(orig_udg))  php_stream_xport_register("udg", PHASYNC_G(orig_udg));
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
	if (PHASYNC_G(orig_popen) && (f = phasync_find_ifunc("popen", sizeof("popen") - 1))) {
		f->handler = PHASYNC_G(orig_popen);
	}
	if (PHASYNC_G(orig_exec) && (f = phasync_find_ifunc("exec", sizeof("exec") - 1))) {
		f->handler = PHASYNC_G(orig_exec);
	}
	if (PHASYNC_G(orig_system) && (f = phasync_find_ifunc("system", sizeof("system") - 1))) {
		f->handler = PHASYNC_G(orig_system);
	}
	if (PHASYNC_G(orig_passthru) && (f = phasync_find_ifunc("passthru", sizeof("passthru") - 1))) {
		f->handler = PHASYNC_G(orig_passthru);
	}
	if (PHASYNC_G(orig_shell_exec) && (f = phasync_find_ifunc("shell_exec", sizeof("shell_exec") - 1))) {
		f->handler = PHASYNC_G(orig_shell_exec);
	}
	if (PHASYNC_G(orig_proc_close) && (f = phasync_find_ifunc("proc_close", sizeof("proc_close") - 1))) {
		f->handler = PHASYNC_G(orig_proc_close);
	}
	if (PHASYNC_G(orig_curl_exec) && (f = phasync_find_ifunc("curl_exec", sizeof("curl_exec") - 1))) {
		f->handler = PHASYNC_G(orig_curl_exec);
	}
	if (PHASYNC_G(orig_sem_acquire) && (f = phasync_find_ifunc("sem_acquire", sizeof("sem_acquire") - 1))) {
		f->handler = PHASYNC_G(orig_sem_acquire);
	}
	if (PHASYNC_G(orig_msg_receive) && (f = phasync_find_ifunc("msg_receive", sizeof("msg_receive") - 1))) {
		f->handler = PHASYNC_G(orig_msg_receive);
	}
	if (PHASYNC_G(orig_curl_multi_select) && (f = phasync_find_ifunc("curl_multi_select", sizeof("curl_multi_select") - 1))) {
		f->handler = PHASYNC_G(orig_curl_multi_select);
	}
	if (PHASYNC_G(orig_socket_connect) && (f = phasync_find_ifunc("socket_connect", sizeof("socket_connect") - 1))) {
		f->handler = PHASYNC_G(orig_socket_connect);
	}
	if (PHASYNC_G(orig_socket_close) && (f = phasync_find_ifunc("socket_close", sizeof("socket_close") - 1))) {
		f->handler = PHASYNC_G(orig_socket_close);
	}
	for (size_t i = 0; i < PHASYNC_SOCK_NFUNCS; i++) {
		if (PHASYNC_G(orig_sock)[i] && (f = phasync_find_ifunc(phasync_sock_funcs[i].name, strlen(phasync_sock_funcs[i].name)))) {
			f->handler = PHASYNC_G(orig_sock)[i];
		}
	}
	zend_hash_clean(&PHASYNC_G(sock_hooks));
	if (PHASYNC_G(orig_pcntl_waitpid) && (f = phasync_find_ifunc("pcntl_waitpid", sizeof("pcntl_waitpid") - 1))) {
		f->handler = PHASYNC_G(orig_pcntl_waitpid);
	}
	if (PHASYNC_G(orig_pcntl_wait) && (f = phasync_find_ifunc("pcntl_wait", sizeof("pcntl_wait") - 1))) {
		f->handler = PHASYNC_G(orig_pcntl_wait);
	}
	for (size_t i = 0; i < PHASYNC_FS_NFUNCS; i++) {
		if (PHASYNC_G(orig_fs)[i] && (f = phasync_find_ifunc(phasync_fs_funcs[i].name, strlen(phasync_fs_funcs[i].name)))) {
			f->handler = PHASYNC_G(orig_fs)[i];
		}
	}
	zend_hash_clean(&PHASYNC_G(fs_hooks));
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
	if (PHASYNC_G(orig_fclose) && (f = phasync_find_ifunc("fclose", sizeof("fclose") - 1))) {
		f->handler = PHASYNC_G(orig_fclose);
	}
#if PHP_VERSION_ID >= 80400
	if (PHASYNC_G(orig_exit)) {
		if ((f = phasync_find_ifunc("exit", sizeof("exit") - 1))) {
			f->handler = PHASYNC_G(orig_exit);
		}
		if ((f = phasync_find_ifunc("die", sizeof("die") - 1)) && f->handler == phasync_exit_override) {
			f->handler = PHASYNC_G(orig_exit);
		}
	}
#endif
	if (PHASYNC_G(orig_connection_aborted) && (f = phasync_find_ifunc("connection_aborted", sizeof("connection_aborted") - 1))) {
		f->handler = PHASYNC_G(orig_connection_aborted);
	}
	if (PHASYNC_G(orig_connection_status) && (f = phasync_find_ifunc("connection_status", sizeof("connection_status") - 1))) {
		f->handler = PHASYNC_G(orig_connection_status);
	}
	PHASYNC_G(hooks_installed) = 0;
}

/* ---- manage(): scoped handler activation --------------------------------- */

static void phasync_wrap_existing_streams(bool include_files);

ZEND_FUNCTION(phasync_ext_manage)
{
	zval *code, *poller, *sleep;
	zend_string *timeout_name;
	zend_class_entry *timeout_ce;
	phasync_scope frame;
	zval retval;

	ZEND_PARSE_PARAMETERS_START(4, 4)
		Z_PARAM_OBJECT_OF_CLASS(code, zend_ce_closure)
		Z_PARAM_OBJECT_OF_CLASS(poller, phasync_poller_ce)
		Z_PARAM_OBJECT_OF_CLASS(sleep, zend_ce_closure)
		Z_PARAM_STR(timeout_name)
	ZEND_PARSE_PARAMETERS_END();

	/* park() throwing an instance of this class (subclasses included) after the
	 * native timeout ran out ends the wait the way a native timeout does. */
	timeout_ce = zend_lookup_class(timeout_name);
	if (timeout_ce == NULL || !instanceof_function(timeout_ce, zend_ce_throwable)) {
		if (!EG(exception)) {
			zend_argument_value_error(4, "must be the name of an existing Throwable class");
		}
		RETURN_THROWS();
	}

	/* Push this scope (owned references), linked to the enclosing one. */
	ZVAL_COPY(&frame.poller_zv, poller);
	frame.poller = phasync_poller_from(Z_OBJ_P(poller));
	ZVAL_COPY(&frame.sleep, sleep);
	frame.timeout_ce = timeout_ce;
	frame.prev = PHASYNC_G(scope_top);
	PHASYNC_G(scope_top) = &frame;

	phasync_install_hooks();   /* idempotent; first manage() of the request installs */
	phasync_holds_release();
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
		zval_ptr_dtor(&frame.poller_zv);
		zval_ptr_dtor(&frame.sleep);
		zend_bailout();
	} zend_end_try();

	PHASYNC_G(scope_top) = frame.prev;
	zval_ptr_dtor(&frame.poller_zv);
	zval_ptr_dtor(&frame.sleep);

	if (Z_ISUNDEF(retval)) {
		RETURN_NULL();   /* $code threw (exception pending) or returned nothing */
	}
	RETURN_COPY_VALUE(&retval);
}

/* ---- phasync\ext\Poller -------------------------------------------------- */

static zend_object *phasync_poller_create(zend_class_entry *ce)
{
	phasync_poller *p = zend_object_alloc(sizeof(phasync_poller), ce);

	zend_object_std_init(&p->std, ce);
	object_properties_init(&p->std, ce);
	p->std.handlers = &phasync_poller_handlers;
	p->epfd = -1;
	p->fork_gen = phasync_fork_gen;
	ZVAL_UNDEF(&p->get_slot);
	ZVAL_UNDEF(&p->park);
	ZVAL_UNDEF(&p->unpark);
	zend_hash_init(&p->regs, 16, NULL, phasync_reg_dtor, 1);
	p->next = phasync_pollers;
	phasync_pollers = p;
	return &p->std;
}

/* Waiters still parked through a freed Poller are never unparked: their waits
 * run to their timeouts (a waiter keeps the Poller alive while it waits, so this
 * happens only to the loop's own parked coroutines). In a fork()ed child only
 * the child's copies of the descriptors are closed. */
static void phasync_poller_free(zend_object *obj)
{
	phasync_poller *p = phasync_poller_from(obj);

	for (phasync_poller **pp = &phasync_pollers; *pp; pp = &(*pp)->next) {
		if (*pp == p) {
			*pp = p->next;
			break;
		}
	}
	zend_hash_destroy(&p->regs);         /* closing the epoll set drops the registrations */
	if (p->epfd >= 0) {
		close(p->epfd);
	}
	if (p->chan) {
		if (p->fork_gen == phasync_fork_gen) {
			phasync_chan_release(p->chan);
		} else {
			close(p->chan->evfd);        /* its mutex may be the parent's, held at fork */
		}
	}
	zval_ptr_dtor(&p->get_slot);
	zval_ptr_dtor(&p->park);
	zval_ptr_dtor(&p->unpark);
	zend_object_std_dtor(obj);
}

/* The closures usually capture the loop that owns the Poller: let the cycle
 * collector see them. */
static HashTable *phasync_poller_get_gc(zend_object *obj, zval **table, int *n)
{
	phasync_poller *p = phasync_poller_from(obj);

	*table = &p->get_slot;               /* get_slot, park, unpark are adjacent */
	*n = 3;
	return zend_std_get_properties(obj);
}

ZEND_METHOD(phasync_ext_Poller, __construct)
{
	zval *get_slot, *park, *unpark;
	phasync_poller *p = phasync_poller_from(Z_OBJ_P(ZEND_THIS));
	phasync_chan *ch;
	struct epoll_event e;

	ZEND_PARSE_PARAMETERS_START(3, 3)
		Z_PARAM_OBJECT_OF_CLASS(get_slot, zend_ce_closure)
		Z_PARAM_OBJECT_OF_CLASS(park, zend_ce_closure)
		Z_PARAM_OBJECT_OF_CLASS(unpark, zend_ce_closure)
	ZEND_PARSE_PARAMETERS_END();

	if (p->epfd >= 0) {
		zend_throw_error(NULL, "The Poller is already constructed");
		RETURN_THROWS();
	}
	ch = calloc(1, sizeof(*ch));
	p->epfd = epoll_create1(EPOLL_CLOEXEC);
	if (ch) {
		ch->evfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	}
	if (!ch || p->epfd < 0 || ch->evfd < 0) {
		int err = errno;
		if (ch) {
			if (ch->evfd >= 0) close(ch->evfd);
			free(ch);
		}
		zend_throw_error(NULL, "Unable to create the epoll set: %s", strerror(err));
		RETURN_THROWS();
	}
	pthread_mutex_init(&ch->mutex, NULL);
	ch->refs = 1;
	p->chan = ch;
	e.events = EPOLLIN;
	e.data.u64 = PHASYNC_EP_COMPLETIONS;
	epoll_ctl(p->epfd, EPOLL_CTL_ADD, ch->evfd, &e);
	ZVAL_COPY(&p->get_slot, get_slot);
	ZVAL_COPY(&p->park, park);
	ZVAL_COPY(&p->unpark, unpark);
}

static phasync_poller *phasync_this_poller(zval *this_zv)
{
	phasync_poller *p = phasync_poller_from(Z_OBJ_P(this_zv));

	if (p->epfd < 0) {
		zend_throw_error(NULL, "The Poller is not constructed");
		return NULL;
	}
	return phasync_poller_usable(p) ? p : NULL;
}

/* The event loop's one blocking call: wait up to $maxTime seconds (0: don't
 * wait) in one epoll_wait(), then unpark the waiters of what became ready, and
 * of the hooked operations started under a manage() given this Poller whose
 * thread tasks finished. Costs per ready event, never a scan of everything
 * registered. */
ZEND_METHOD(phasync_ext_Poller, poll)
{
	double max_time;
	struct epoll_event ev[256];
	zend_long wake_stack[512], *wake = wake_stack;
	size_t nwake = 0, cap = sizeof(wake_stack) / sizeof(wake_stack[0]);
	phasync_poller *p;
	int ms, n;

	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_DOUBLE(max_time)
	ZEND_PARSE_PARAMETERS_END();

	if ((p = phasync_this_poller(ZEND_THIS)) == NULL) {
		RETURN_THROWS();
	}
	phasync_holds_release();
	if (EG(exception)) {
		RETURN_THROWS();
	}
	if (max_time <= 0 && p->armed == 0) {
		/* The loop polls without waiting on every tick while it has work: with no
		 * descriptor waited on and no finished thread task queued, skip the syscall. */
		size_t queued;
		pthread_mutex_lock(&p->chan->mutex);
		queued = p->chan->len;
		pthread_mutex_unlock(&p->chan->mutex);
		if (queued == 0) {
			return;
		}
	}
	/* Round up, so a wait shorter than a millisecond doesn't spin. */
	ms = max_time <= 0 ? 0 : (max_time >= INT_MAX / 1000 ? -1 : (int) ceil(max_time * 1000));
	n = epoll_wait(p->epfd, ev, sizeof(ev) / sizeof(ev[0]), ms);
	if (n < 0) {
		if (errno == EINTR) {
			return;                          /* a signal: its handler has run */
		}
		zend_throw_error(NULL, "epoll_wait() failed: %s", strerror(errno));
		RETURN_THROWS();
	}

	/* Settle every registration first, then call unpark() (PHP) for each. */
	for (int i = 0; i < n; i++) {
		if (ev[i].data.u64 == PHASYNC_EP_COMPLETIONS) {
			phasync_chan *ch = p->chan;
			uint64_t count;
			(void) !read(ch->evfd, &count, sizeof(count));
			pthread_mutex_lock(&ch->mutex);
			if (nwake + ch->len > cap) {
				cap = nwake + ch->len;
				wake = wake == wake_stack ? memcpy(emalloc(cap * sizeof(*wake)), wake_stack, nwake * sizeof(*wake))
				                          : erealloc(wake, cap * sizeof(*wake));
			}
			memcpy(wake + nwake, ch->queue, ch->len * sizeof(*wake));
			nwake += ch->len;
			ch->len = 0;
			pthread_mutex_unlock(&ch->mutex);
		} else {
			phasync_reg *r = zend_hash_index_find_ptr(&p->regs, (zend_ulong) ev[i].data.u64);
			uint32_t got = ev[i].events;
			bool rd, wr;

			if (r == NULL) {
				continue;
			}
			/* Ready also when closed or failed: the op then reports it as PHP does. */
			rd = r->slot[0] >= 0 && (got & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR));
			wr = r->slot[1] >= 0 && (got & (EPOLLOUT | EPOLLHUP | EPOLLERR));
			if (r->hooked) {
				/* Level-triggered: disarm what fired with nobody waiting for it. */
				uint32_t stray = (r->slot[0] < 0 && (got & (EPOLLIN | EPOLLRDHUP)) ? EPOLLIN | EPOLLRDHUP : 0)
				               | (r->slot[1] < 0 && (got & EPOLLOUT) ? EPOLLOUT : 0);
				if ((got & (EPOLLHUP | EPOLLERR)) && !rd && !wr) {
					epoll_ctl(p->epfd, EPOLL_CTL_DEL, r->fd, NULL);
					r->added = false;
					r->events = 0;
				} else if (stray & r->events) {
					if ((r->events & ~stray) == 0) {
						epoll_ctl(p->epfd, EPOLL_CTL_DEL, r->fd, NULL);
						r->added = false;
						r->events = 0;
					} else {
						phasync_reg_ctl(p, r, r->events & ~stray, false);
					}
				}
			}
			if (nwake + 2 > cap) {
				cap *= 2;
				wake = wake == wake_stack ? memcpy(emalloc(cap * sizeof(*wake)), wake_stack, nwake * sizeof(*wake))
				                          : erealloc(wake, cap * sizeof(*wake));
			}
			if (rd) {
				wake[nwake++] = r->slot[0];
				r->slot[0] = -1;
				p->armed--;
			}
			if (wr) {
				wake[nwake++] = r->slot[1];
				r->slot[1] = -1;
				p->armed--;
			}
			if (!r->hooked && (r->slot[0] >= 0 || r->slot[1] >= 0)) {
				phasync_reg_arm(p, r);       /* one-shot: the other direction still waits */
			}
		}
	}
	for (size_t i = 0; i < nwake && !EG(exception); i++) {
		zval arg, retval;
		ZVAL_LONG(&arg, wake[i]);
		ZVAL_UNDEF(&retval);
		call_user_function(NULL, NULL, &p->unpark, &retval, 1, &arg);
		zval_ptr_dtor(&retval);
	}
	if (wake != wake_stack) {
		efree(wake);
	}
}

static void phasync_poller_wait_method(INTERNAL_FUNCTION_PARAMETERS, int dir)
{
	zval *zstream;
	double timeout = DBL_MAX;
	php_stream *stream;
	phasync_poller *p;

	ZEND_PARSE_PARAMETERS_START(1, 2)
		Z_PARAM_RESOURCE(zstream)
		Z_PARAM_OPTIONAL
		Z_PARAM_DOUBLE(timeout)
	ZEND_PARSE_PARAMETERS_END();

	php_stream_from_zval(stream, zstream);
	if ((p = phasync_this_poller(ZEND_THIS)) == NULL) {
		RETURN_THROWS();
	}
	if (EG(active_fiber) == NULL) {
		zend_throw_error(NULL, "Poller::%s() must be called in a coroutine", dir == PHASYNC_READ ? "readable" : "writable");
		RETURN_THROWS();
	}
	/* The loop's exceptions (a timeout, a cancellation) reach the caller as they are. */
	phasync_poller_wait(p, dir, stream, phasync_stream_fd(stream), timeout >= DBL_MAX ? INFINITY : timeout, NULL, true);
}

ZEND_METHOD(phasync_ext_Poller, readable)
{
	phasync_poller_wait_method(INTERNAL_FUNCTION_PARAM_PASSTHRU, PHASYNC_READ);
}

ZEND_METHOD(phasync_ext_Poller, writable)
{
	phasync_poller_wait_method(INTERNAL_FUNCTION_PARAM_PASSTHRU, PHASYNC_WRITE);
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

	/* The scheduler's own select must never suspend, but casting a stream to its
	 * fd can do I/O (php://temp spills its buffer to a file): run that inline. */
	zend_hash_init(&events_by_fd, 8, NULL, NULL, 0);
	PHASYNC_G(no_suspend)++;
	sets += phasync_collect(r_array, &events_by_fd, POLLIN);
	sets += phasync_collect(w_array, &events_by_fd, POLLOUT);
	sets += phasync_collect(e_array, &events_by_fd, POLLPRI);
	PHASYNC_G(no_suspend)--;

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


/* set_preempt_function(): every backward jump (a loop's back-edge, a backward
 * goto, foreach's continue) is compiled with
 *     if (\phasync\ext\__PREEMPT_DUE) \phasync\ext\checkpoint();
 * right before it (phasync_inject_checkpoints()). The constant is registered
 * per request, without CONST_PERSISTENT, so neither the compiler nor opcache
 * substitutes it; FETCH_CONSTANT reads it through its run-time cache slot, and
 * both JITs compile that into loads and a compare of its type byte, without a
 * call. A timer thread raises it at most every interval by storing IS_TRUE into
 * its type_info (an aligned 32-bit store, single-copy atomic on x86-64 and
 * aarch64; no data is published through it, so relaxed ordering is enough: the
 * checkpoint re-checks `pending` and takes the mutex). The next checkpoint that
 * runs calls the closure if the stack allows it. The interval counts from the
 * moment the closure is due there: the thread raises one request at a time.
 *
 * A checkpoint the stack rules reject lowers the flag (so the checkpoints that
 * follow cost a load again) and the thread raises it again after
 * PHASYNC_PREEMPT_RETRY. */
#define PHASYNC_PREEMPT_RETRY_NS 250000L    /* 0.25 ms */

static zend_always_inline void phasync_flag_store(uint32_t *flag, uint32_t type)
{
	__atomic_store_n(flag, type, __ATOMIC_RELAXED);
}

static zend_always_inline bool phasync_flag_raised(void)
{
	return __atomic_load_n(&Z_TYPE_INFO_P(PHASYNC_G(preempt_due)), __ATOMIC_RELAXED) == IS_TRUE;
}

typedef struct phasync_preempt {
	pthread_mutex_t   mutex;
	pthread_cond_t    cond;
	pthread_t         thread;
	uint32_t         *flag;           /* type_info of the owning PHP thread's __PREEMPT_DUE */
	zend_atomic_bool  pending;        /* raised, not yet taken at a checkpoint    */
	bool              stop;
	double            interval;
	struct timespec   since;          /* start of the current interval (CLOCK_MONOTONIC) */
} phasync_preempt;

/* What the checkpoint needs to know of an op_array, built on first need and kept
 * for the request in its run-time cache (writable also for opcache's immutable
 * op_arrays), in the arena that lives as long. */
typedef struct phasync_loop_info {
	bool     no_preempt;              /* #[phasync\Uninterruptible], or a destructor */
} phasync_loop_info;

static int phasync_preempt_handle;    /* op_array extension slot: its phasync_loop_info */
static zend_string *phasync_uninterruptible_lcname;   /* the attribute, as attributes are keyed */

static zend_always_inline void phasync_ts_add_ns(struct timespec *ts, long ns)
{
	ts->tv_nsec += ns;
	while (ts->tv_nsec >= 1000000000L) {
		ts->tv_sec++;
		ts->tv_nsec -= 1000000000L;
	}
}

static void *phasync_preempt_main(void *arg)
{
	phasync_preempt *p = arg;
	struct timespec now, deadline;

	pthread_mutex_lock(&p->mutex);
	while (!p->stop) {
		if (zend_atomic_bool_load_ex(&p->pending)) {
			/* Not taken yet: no checkpoint ran, or the stack rules rejected
			 * it; raise the flag again shortly. */
			clock_gettime(CLOCK_MONOTONIC, &deadline);
			phasync_ts_add_ns(&deadline, PHASYNC_PREEMPT_RETRY_NS);
			if (pthread_cond_timedwait(&p->cond, &p->mutex, &deadline) == ETIMEDOUT
			 && zend_atomic_bool_load_ex(&p->pending)) {
				phasync_flag_store(p->flag, IS_TRUE);
			}
			continue;
		}
		deadline = p->since;
		deadline.tv_sec += (time_t) p->interval;
		phasync_ts_add_ns(&deadline, (long) ((p->interval - floor(p->interval)) * 1e9));
		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec > deadline.tv_sec || (now.tv_sec == deadline.tv_sec && now.tv_nsec >= deadline.tv_nsec)) {
			zend_atomic_bool_store_ex(&p->pending, true);
			phasync_flag_store(p->flag, IS_TRUE);
			continue;
		}
		pthread_cond_timedwait(&p->cond, &p->mutex, &deadline);
	}
	pthread_mutex_unlock(&p->mutex);
	return NULL;
}

/* Start a timer for this PHP thread; NULL if no thread could be created. The
 * thread blocks every signal, so process signals (pcntl, SIGPROF of
 * max_execution_time) keep going to PHP's thread. */
static phasync_preempt *phasync_preempt_start(double interval)
{
	phasync_preempt *p = calloc(1, sizeof(*p));
	pthread_condattr_t ca;
	sigset_t all, old;
	int rc;

	if (!p) {
		return NULL;
	}
	pthread_mutex_init(&p->mutex, NULL);
	pthread_condattr_init(&ca);
	pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
	pthread_cond_init(&p->cond, &ca);
	pthread_condattr_destroy(&ca);
	p->flag = &Z_TYPE_INFO_P(PHASYNC_G(preempt_due));
	zend_atomic_bool_init(&p->pending, false);
	p->interval = interval;
	clock_gettime(CLOCK_MONOTONIC, &p->since);
	sigfillset(&all);
	pthread_sigmask(SIG_SETMASK, &all, &old);
	rc = pthread_create(&p->thread, NULL, phasync_preempt_main, p);
	pthread_sigmask(SIG_SETMASK, &old, NULL);
	if (rc != 0) {
		pthread_cond_destroy(&p->cond);
		pthread_mutex_destroy(&p->mutex);
		free(p);
		return NULL;
	}
	return p;
}

static void phasync_preempt_stop(phasync_preempt *p)
{
	pthread_mutex_lock(&p->mutex);
	p->stop = true;
	pthread_cond_signal(&p->cond);
	pthread_mutex_unlock(&p->mutex);
	pthread_join(p->thread, NULL);
	pthread_cond_destroy(&p->cond);
	pthread_mutex_destroy(&p->mutex);
	free(p);
	phasync_flag_store(&Z_TYPE_INFO_P(PHASYNC_G(preempt_due)), IS_FALSE);
}

/* fork(): the child has no timer thread. It gets one of its own, with the same
 * interval; the parent's mutex is held across fork() so the child's copy is in
 * a known state (owned by the forking thread, which the child has). */
static void phasync_preempt_fork_prepare(void)
{
	if (PHASYNC_G(preempt)) {
		pthread_mutex_lock(&PHASYNC_G(preempt)->mutex);
	}
}

static void phasync_preempt_fork_parent(void)
{
	if (PHASYNC_G(preempt)) {
		pthread_mutex_unlock(&PHASYNC_G(preempt)->mutex);
	}
}

static void phasync_preempt_fork_child(void)
{
	phasync_preempt *old = PHASYNC_G(preempt);

	if (old) {
		/* old's cond may record the parent's timer thread as a waiter: it is
		 * dropped as plain memory, not destroyed. If no thread can be created,
		 * the child runs without preemption. */
		PHASYNC_G(preempt) = phasync_preempt_start(old->interval);
		free(old);
	}
}

static phasync_loop_info *phasync_loop_info_of(const zend_op_array *op_array)
{
	phasync_loop_info *info = ZEND_OP_ARRAY_EXTENSION(op_array, phasync_preempt_handle);

	if (info) {
		return info;
	}
	info = zend_arena_alloc(&CG(arena), sizeof(*info));
	info->no_preempt = (op_array->attributes
		&& zend_get_attribute(op_array->attributes, phasync_uninterruptible_lcname))
		|| (op_array->scope && op_array->function_name
		 && zend_string_equals_literal_ci(op_array->function_name, "__destruct"));
	ZEND_OP_ARRAY_EXTENSION(op_array, phasync_preempt_handle) = info;
	return info;
}

/* Preemption happens only at checkpoints (between loop iterations), in PHP code
 * called from PHP code: on the stack, from execute_data (the frame whose
 * checkpoint runs) down to the fiber's first frame (or the script's own code
 * outside fibers), is no C function (a callback of usort(), an output handler,
 * a session handler ...), no function the engine called in the middle of an
 * operation rather than at a call (an error handler, a magic method,
 * __toString(), an Iterator's methods driven by foreach, an autoloader, the
 * preempt closure itself ...), no destructor, and no function with
 * #[phasync\Uninterruptible]. Outside fibers, a function at the bottom of the
 * stack was called from C (an exception handler, a shutdown function). */
static bool phasync_preemptible(const zend_execute_data *execute_data)
{
	const zend_execute_data *bottom = EG(active_fiber) ? EG(active_fiber)->stack_bottom : NULL;

	for (const zend_execute_data *ex = execute_data, *prev; ex != bottom; ex = prev) {
		prev = ex->prev_execute_data;
		if (!ex->func || !ZEND_USER_CODE(ex->func->type)) {
			return false;   /* a C function */
		}
		if (ex->func->op_array.fn_flags & ZEND_ACC_CALL_VIA_TRAMPOLINE) {
			continue;
		}
		if (phasync_loop_info_of(&ex->func->op_array)->no_preempt) {
			return false;
		}
		if (!prev) {
			return !ex->func->op_array.function_name;   /* the script's own code */
		}
		if (prev != bottom && !(ZEND_CALL_INFO(ex) & ZEND_CALL_GENERATOR)
		 && prev->func && ZEND_USER_CODE(prev->func->type)) {
			switch (prev->opline->opcode) {
				case ZEND_DO_FCALL:
				case ZEND_DO_UCALL:
				case ZEND_DO_FCALL_BY_NAME:
				case ZEND_INCLUDE_OR_EVAL:
					break;
				default:
					return false;   /* called by the engine, not at a call */
			}
		}
	}
	return true;
}

/* A raised checkpoint: ex is the PHP frame it is in. With own_frame, the
 * checkpoint was called with a frame of its own (EG(current_execute_data)),
 * which is left out of the stack while the closure runs; without, the frame's
 * opline is the frameless call, moved past it while the closure runs so no
 * backtrace shows it. */
static zend_never_inline void phasync_checkpoint_hit(zend_execute_data *ex, bool own_frame)
{
	phasync_preempt *p = PHASYNC_G(preempt);
	zend_execute_data *saved_ex;
	const zend_op *saved_op;
	zval fn, retval;

	phasync_flag_store(&Z_TYPE_INFO_P(PHASYNC_G(preempt_due)), IS_FALSE);
	if (!p || !zend_atomic_bool_load_ex(&p->pending)) {
		return;
	}
	/* Uninterruptible here, or an exception is on its way: the thread asks
	 * again after PHASYNC_PREEMPT_RETRY. */
	if (EG(exception) || !ex || !phasync_preemptible(ex)) {
		return;
	}
	pthread_mutex_lock(&p->mutex);
	zend_atomic_bool_store_ex(&p->pending, false);
	clock_gettime(CLOCK_MONOTONIC, &p->since);
	pthread_cond_signal(&p->cond);
	pthread_mutex_unlock(&p->mutex);

	/* Where Fiber::suspend() would throw (pcntl handlers, a fiber being
	 * destroyed), skip: the next interval gets it. */
	if (zend_fiber_switch_blocked() || phasync_unwinding()) {
		return;
	}
	saved_ex = EG(current_execute_data);
	saved_op = ex->opline;
	if (own_frame) {
		EG(current_execute_data) = ex;
	} else {
		ex->opline = saved_op + 1;
	}
	ZVAL_COPY(&fn, &PHASYNC_G(preempt_fn));   /* it may replace itself */
	call_user_function(NULL, NULL, &fn, &retval, 0, NULL);
	zval_ptr_dtor(&retval);
	zval_ptr_dtor(&fn);
	if (own_frame) {
		EG(current_execute_data) = saved_ex;
	} else if (!EG(exception)) {
		ex->opline = saved_op;
	} else if (EG(opline_before_exception) == saved_op + 1) {
		EG(opline_before_exception) = saved_op;
	}
}

/* The checkpoint, called when __PREEMPT_DUE was seen raised. PHP 8.4+ calls it
 * frameless and has it in no function table; older PHP calls it with a frame and
 * by name, so PHP code can call it too (hence the check). */
ZEND_FUNCTION(phasync_checkpoint)
{
	if (EXPECTED(!phasync_flag_raised())) {
		return;
	}
	phasync_checkpoint_hit(execute_data->prev_execute_data, true);
}

#if PHP_VERSION_ID >= 80400
ZEND_FRAMELESS_FUNCTION(phasync_checkpoint, 0)
{
	if (EXPECTED(!phasync_flag_raised())) {
		return;
	}
	phasync_checkpoint_hit(EG(current_execute_data), false);
}

static const zend_frameless_function_info phasync_checkpoint_flf[] = {
	{ ZEND_FRAMELESS_FUNCTION_NAME(phasync_checkpoint, 0), 0 },
	{ 0 },
};
static uint32_t phasync_checkpoint_flf_offset;
#endif
static zend_function *phasync_checkpoint_func;

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_phasync_checkpoint, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

static const zend_function_entry phasync_checkpoint_functions[] = {
	{ .fname = "phasync\\ext\\checkpoint", .handler = ZEND_FN(phasync_checkpoint),
	  .arg_info = arginfo_phasync_checkpoint, .num_args = 0, .flags = 0,
#if PHP_VERSION_ID >= 80400
	  .frameless_function_infos = phasync_checkpoint_flf,
#endif
	},
	ZEND_FE_END
};
static zend_string *phasync_checkpoint_name;
static zend_string *phasync_preempt_due_name;

/* Compile time, before opcache optimizes and caches the op_array: pass_two()
 * calls this first, while jump targets are still opline numbers and break,
 * continue and goto still unresolved. */

/* Where a raw jump op goes, or -1 if it can't jump backward. */
static uint32_t phasync_raw_backward_target(const zend_op_array *op_array, const zend_op *op)
{
	switch (op->opcode) {
		case ZEND_JMP:
			return op->op1.opline_num;
		case ZEND_JMPZ:
		case ZEND_JMPNZ:
		case ZEND_JMPZ_EX:
		case ZEND_JMPNZ_EX:
			return op->op2.opline_num;
		case ZEND_GOTO: {
			zval *label = CT_CONSTANT_EX(op_array, op->op2.constant);
			zend_label *dest = CG(context).labels ? zend_hash_find_ptr(CG(context).labels, Z_STR_P(label)) : NULL;

			return dest ? dest->opline_num : (uint32_t) -1;
		}
		case ZEND_CONT: {
			int nest_levels = op->op2.num, array_offset = op->op1.num;
			zend_brk_cont_element *jmp_to;

			do {
				jmp_to = &CG(context).brk_cont_array[array_offset];
				if (nest_levels > 1) {
					array_offset = jmp_to->parent;
				}
			} while (--nest_levels > 0);
			return jmp_to->cont;
		}
		default:
			return (uint32_t) -1;
	}
}

/* Apply map to every opline number a raw op_array refers to. */
static void phasync_raw_map_targets(zend_op_array *op_array, uint32_t (*map)(uint32_t, void *), void *arg)
{
	for (uint32_t i = 0; i < op_array->last; i++) {
		zend_op *op = &op_array->opcodes[i];

		switch (op->opcode) {
			case ZEND_JMP:
				op->op1.opline_num = map(op->op1.opline_num, arg);
				break;
			case ZEND_CATCH:
				if (op->extended_value & ZEND_LAST_CATCH) {
					break;
				}
				ZEND_FALLTHROUGH;
			case ZEND_JMPZ:
			case ZEND_JMPNZ:
			case ZEND_JMPZ_EX:
			case ZEND_JMPNZ_EX:
			case ZEND_JMP_SET:
			case ZEND_COALESCE:
			case ZEND_FE_RESET_R:
			case ZEND_FE_RESET_RW:
			case ZEND_JMP_NULL:
			case ZEND_ASSERT_CHECK:
#if PHP_VERSION_ID >= 80300
			case ZEND_BIND_INIT_STATIC_OR_JMP:
#endif
#if PHP_VERSION_ID >= 80400
			case ZEND_JMP_FRAMELESS:
#endif
				op->op2.opline_num = map(op->op2.opline_num, arg);
				break;
			case ZEND_FE_FETCH_R:
			case ZEND_FE_FETCH_RW:
				op->extended_value = map(op->extended_value, arg);
				break;
			case ZEND_SWITCH_LONG:
			case ZEND_SWITCH_STRING:
			case ZEND_MATCH: {
				zval *zv;

				ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(CT_CONSTANT_EX(op_array, op->op2.constant)), zv) {
					Z_LVAL_P(zv) = map((uint32_t) Z_LVAL_P(zv), arg);
				} ZEND_HASH_FOREACH_END();
				op->extended_value = map(op->extended_value, arg);
				break;
			}
		}
	}
	for (int i = 0; i < op_array->last_try_catch; i++) {
		zend_try_catch_element *tc = &op_array->try_catch_array[i];

		tc->try_op = map(tc->try_op, arg);
		tc->catch_op = map(tc->catch_op, arg);
		tc->finally_op = map(tc->finally_op, arg);
		tc->finally_end = map(tc->finally_end, arg);
	}
	for (int i = 0; i < CG(context).last_brk_cont; i++) {
		zend_brk_cont_element *bc = &CG(context).brk_cont_array[i];

		if (bc->start >= 0) {
			bc->start = (int) map((uint32_t) bc->start, arg);
		}
		if (bc->cont >= 0) {
			bc->cont = (int) map((uint32_t) bc->cont, arg);
		}
		if (bc->brk >= 0) {
			bc->brk = (int) map((uint32_t) bc->brk, arg);
		}
	}
	if (CG(context).labels) {
		zend_label *label;

		ZEND_HASH_FOREACH_PTR(CG(context).labels, label) {
			label->opline_num = map(label->opline_num, arg);
		} ZEND_HASH_FOREACH_END();
	}
}

static uint32_t phasync_mark_target(uint32_t t, void *arg)
{
	uint32_t *targeted = arg;

	if (t != (uint32_t) -1) {
		targeted[t / 32] |= 1u << (t % 32);
	}
	return t;
}

typedef struct {
	const uint32_t *at;   /* sorted insertion points */
	uint32_t n, k;        /* how many, ops per checkpoint */
} phasync_shift;

/* A target moves past the checkpoints inserted before it; one at an insertion
 * point lands on that point's checkpoint. */
static uint32_t phasync_shift_target(uint32_t t, void *arg)
{
	const phasync_shift *s = arg;
	uint32_t lo = 0, hi = s->n;

	while (lo < hi) {   /* how many insertion points are < t */
		uint32_t mid = (lo + hi) / 2;

		if (s->at[mid] < t) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	return t + lo * s->k;
}

/* FETCH_CONSTANT __PREEMPT_DUE, JMPZ past the call, then the call:
 * FRAMELESS_ICALL_0 + FREE on PHP 8.4+, INIT_FCALL + DO_ICALL before. */
#define PHASYNC_CHECKPOINT_OPS 4

/* Append a literal (a string interned for the process) to a raw op_array. */
static uint32_t phasync_add_literal(zend_op_array *op_array, zend_string *str)
{
	uint32_t lit = op_array->last_literal;

	if (lit >= CG(context).literals_size) {
		CG(context).literals_size += 16;
		op_array->literals = erealloc(op_array->literals, CG(context).literals_size * sizeof(zval));
	}
	ZVAL_INTERNED_STR(&op_array->literals[lit], str);
	Z_EXTRA(op_array->literals[lit]) = 0;
	op_array->last_literal++;
	return lit;
}

static void phasync_inject_checkpoints(zend_op_array *op_array)
{
	uint32_t *at, *targeted, n = 0, last = op_array->last, k = PHASYNC_CHECKPOINT_OPS;
	zend_op *opcodes;
	phasync_shift shift;

	for (uint32_t i = 0; i < last; i++) {
		uint32_t t = phasync_raw_backward_target(op_array, &op_array->opcodes[i]);

		if (t <= i) {
			n++;
		}
	}
	if (!n) {
		return;
	}
	at = emalloc(n * sizeof(uint32_t));
	targeted = ecalloc(last / 32 + 1, sizeof(uint32_t));
	phasync_raw_map_targets(op_array, phasync_mark_target, targeted);
	n = 0;
	for (uint32_t i = 0; i < last; i++) {
		const zend_op *op = &op_array->opcodes[i];
		uint32_t t = phasync_raw_backward_target(op_array, op), p = i;

		if (t > i) {
			continue;
		}
		if (op->opcode == ZEND_GOTO) {
			p = i - op->op1.num;   /* before the frees goto may unwind with */
		} else if ((op->opcode == ZEND_JMPZ || op->opcode == ZEND_JMPNZ)
		 && op->op1_type == IS_TMP_VAR && i > 0 && !((targeted[i / 32] >> (i % 32)) & 1)
		 && (op[-1].result_type & IS_TMP_VAR) && op[-1].result.var == op->op1.var) {
			p = i - 1;         /* keep the condition next to its jump (smart branch) */
		}
		at[n++] = p;
	}
	efree(targeted);

	shift = (phasync_shift) { at, n, k };
	phasync_raw_map_targets(op_array, phasync_shift_target, &shift);

	opcodes = emalloc(sizeof(zend_op) * (last + n * k));
	for (uint32_t i = 0, j = 0, o = 0; i < last; i++) {
		while (j < n && at[j] == i) {
			const zend_op *jump = &op_array->opcodes[i];
			zend_op *cp = &opcodes[o];

			memset(cp, 0, sizeof(zend_op) * k);
			for (uint32_t c = 0; c < k; c++) {
				SET_UNUSED(cp[c].op1);
				SET_UNUSED(cp[c].op2);
				SET_UNUSED(cp[c].result);
				cp[c].lineno = jump->lineno;
			}
			/* FETCH_CONSTANT expects two literals: the name as written and the
			 * one to look up (namespace lowercased), the same here. */
			cp[0].opcode = ZEND_FETCH_CONSTANT;
			cp[0].op1.num = 0;              /* a fully qualified name */
			cp[0].op2_type = IS_CONST;
			cp[0].op2.constant = phasync_add_literal(op_array, phasync_preempt_due_name);
			phasync_add_literal(op_array, phasync_preempt_due_name);
			cp[0].result_type = IS_TMP_VAR;
			cp[0].result.var = op_array->T;
			cp[0].extended_value = op_array->cache_size;
			op_array->cache_size += sizeof(void *);
			cp[1].opcode = ZEND_JMPZ;
			cp[1].op1_type = IS_TMP_VAR;
			cp[1].op1.var = op_array->T++;
			cp[1].op2.opline_num = o + k;   /* the jump itself */
#if PHP_VERSION_ID >= 80400
			cp[2].opcode = ZEND_FRAMELESS_ICALL_0;
			cp[2].extended_value = phasync_checkpoint_flf_offset;
			cp[2].result_type = IS_TMP_VAR;
			cp[2].result.var = op_array->T;
			cp[3].opcode = ZEND_FREE;
			cp[3].op1_type = IS_TMP_VAR;
			cp[3].op1.var = op_array->T++;
#else
			cp[2].opcode = ZEND_INIT_FCALL;
			cp[2].op1.num = zend_vm_calc_used_stack(0, phasync_checkpoint_func);
			cp[2].op2_type = IS_CONST;
			cp[2].op2.constant = phasync_add_literal(op_array, phasync_checkpoint_name);
			cp[2].result.num = op_array->cache_size;
			op_array->cache_size += sizeof(void *);
			cp[3].opcode = ZEND_DO_ICALL;
#endif
			o += k;
			j++;
		}
		opcodes[o++] = op_array->opcodes[i];
	}
	efree(op_array->opcodes);
	op_array->opcodes = opcodes;
	op_array->last = last + n * k;
	CG(context).opcodes_size = op_array->last;
	efree(at);
}

static void phasync_op_array_handler(zend_op_array *op_array)
{
	phasync_inject_checkpoints(op_array);
}

static zend_extension phasync_zend_extension = {
	.name = "phasync",
	.version = PHP_PHASYNC_VERSION,
	.author = "phasync",
	.op_array_handler = phasync_op_array_handler,
	.resource_number = -1,
};

/* Registered in MINIT: the checkpoint function and the compile hook. */
static void phasync_checkpoint_startup(void)
{
	zend_register_functions(NULL, phasync_checkpoint_functions, NULL, MODULE_PERSISTENT);
	phasync_checkpoint_name = zend_string_init_interned(ZEND_STRL("phasync\\ext\\checkpoint"), 1);
	phasync_preempt_due_name = zend_string_init_interned(ZEND_STRL("phasync\\ext\\__PREEMPT_DUE"), 1);
	phasync_checkpoint_func = zend_hash_find_ptr(CG(function_table), phasync_checkpoint_name);
#if PHP_VERSION_ID >= 80400
	for (uint32_t i = 0; zend_flf_handlers[i]; i++) {
		if (zend_flf_handlers[i] == (void *) ZEND_FRAMELESS_FUNCTION_NAME(phasync_checkpoint, 0)) {
			phasync_checkpoint_flf_offset = i;
		}
	}
	/* Frameless calls reach it through zend_flf_handlers/zend_flf_functions, not
	 * by name: take it out of the function table (without freeing it, MSHUTDOWN
	 * does), so PHP code can neither see nor call it. */
	{
		dtor_func_t dtor = CG(function_table)->pDestructor;

		CG(function_table)->pDestructor = NULL;
		zend_hash_del(CG(function_table), phasync_checkpoint_name);
		CG(function_table)->pDestructor = dtor;
	}
#endif
	zend_register_extension(&phasync_zend_extension, NULL);
}

ZEND_FUNCTION(phasync_ext_set_preempt_function)
{
	zend_object *fn;
	double interval = 0.1;
	phasync_preempt *p;

	ZEND_PARSE_PARAMETERS_START(1, 2)
		Z_PARAM_OBJ_OF_CLASS_OR_NULL(fn, zend_ce_closure)
		Z_PARAM_OPTIONAL
		Z_PARAM_DOUBLE(interval)
	ZEND_PARSE_PARAMETERS_END();

	if (!(interval > 0) || !zend_finite(interval)) {
		zend_argument_value_error(2, "must be a positive number of seconds");
		RETURN_THROWS();
	}
	p = PHASYNC_G(preempt);
	if (fn && !p) {
		if (!(p = phasync_preempt_start(interval))) {
			zend_throw_error(NULL, "Could not start the preempt timer thread");
			RETURN_THROWS();
		}
		PHASYNC_G(preempt) = p;
	} else if (fn) {
		pthread_mutex_lock(&p->mutex);
		p->interval = interval;
		pthread_cond_signal(&p->cond);
		pthread_mutex_unlock(&p->mutex);
	} else if (p) {
		PHASYNC_G(preempt) = NULL;
		phasync_preempt_stop(p);
	}
	if (Z_TYPE(PHASYNC_G(preempt_fn)) == IS_UNDEF) {
		RETVAL_NULL();
	} else {
		ZVAL_COPY_VALUE(return_value, &PHASYNC_G(preempt_fn));
	}
	if (fn) {
		ZVAL_OBJ_COPY(&PHASYNC_G(preempt_fn), fn);
	} else {
		ZVAL_UNDEF(&PHASYNC_G(preempt_fn));
	}
}


static ZEND_INI_MH(phasync_update_fs_offload)
{
	if (zend_string_equals_literal_ci(new_value, "network")) {
		PHASYNC_G(fs_offload) = PHASYNC_FS_OFFLOAD_NETWORK;
	} else if (zend_string_equals_literal_ci(new_value, "all")) {
		PHASYNC_G(fs_offload) = PHASYNC_FS_OFFLOAD_ALL;
	} else if (zend_string_equals_literal_ci(new_value, "none")) {
		PHASYNC_G(fs_offload) = PHASYNC_FS_OFFLOAD_NONE;
	} else {
		return FAILURE;
	}
	PHASYNC_G(fs_last) = NULL;           /* decided under the old policy */
	return SUCCESS;
}

static ZEND_INI_MH(phasync_update_fs_offload_types)
{
	if (OnUpdateString(entry, new_value, mh_arg1, mh_arg2, mh_arg3, stage) != SUCCESS) {
		return FAILURE;
	}
	if (PHASYNC_G(mountinfo_fd) >= 0) {  /* reload the mount table on next use */
		close(PHASYNC_G(mountinfo_fd));
		PHASYNC_G(mountinfo_fd) = -1;
	}
	PHASYNC_G(fs_last) = NULL;
	return SUCCESS;
}

PHP_INI_BEGIN()
	STD_PHP_INI_ENTRY("phasync.thread_pool_size", "8", PHP_INI_SYSTEM, OnUpdateLong,
		thread_pool_size, zend_phasync_globals, phasync_globals)
	PHP_INI_ENTRY("phasync.fs_offload", "network", PHP_INI_ALL, phasync_update_fs_offload)
	STD_PHP_INI_ENTRY("phasync.fs_offload_types", "", PHP_INI_ALL, phasync_update_fs_offload_types,
		fs_offload_types, zend_phasync_globals, phasync_globals)
PHP_INI_END()

static PHP_MINIT_FUNCTION(phasync)
{
	REGISTER_INI_ENTRIES();
	phasync_poller_ce = register_class_phasync_ext_Poller();
	phasync_poller_ce->create_object = phasync_poller_create;
	memcpy(&phasync_poller_handlers, &std_object_handlers, sizeof(zend_object_handlers));
	phasync_poller_handlers.offset = XtOffsetOf(phasync_poller, std);
	phasync_poller_handlers.free_obj = phasync_poller_free;
	phasync_poller_handlers.get_gc = phasync_poller_get_gc;
	phasync_poller_handlers.clone_obj = NULL;
	pthread_atfork(phasync_preempt_fork_prepare, phasync_preempt_fork_parent, phasync_fork_child);
	phasync_preempt_handle = zend_get_op_array_extension_handle("phasync");
	phasync_checkpoint_startup();
	{
		zend_class_entry *ce = register_class_phasync_Uninterruptible();

		zend_mark_internal_attribute(ce);   /* its targets are checked at compile time */
		phasync_uninterruptible_lcname = zend_string_tolower_ex(ce->name, 1);
	}
	phasync_stdio_read_orig = php_stream_stdio_ops.read;
	phasync_stdio_write_orig = php_stream_stdio_ops.write;
	phasync_stdio_set_option_orig = php_stream_stdio_ops.set_option;
	phasync_stdio_close_orig = php_stream_stdio_ops.close;
	php_stream_stdio_ops.close = phasync_stdio_close;
	php_stream_stdio_ops.read = phasync_stdio_read;
	php_stream_stdio_ops.write = phasync_stdio_write;
	php_stream_stdio_ops.set_option = phasync_stdio_set_option;
	if (strcmp(sapi_module.name, "cli") == 0) {
		phasync_ub_write_orig = sapi_module.ub_write;
		sapi_module.ub_write = phasync_ub_write;
	}
	/* virtualize(): route the SAPI callbacks to the live boundary, if any. A
	 * missing send_headers/flush/read_post behaves as it did (the router falls
	 * back to what PHP does without one); header_handler is only wrapped if the
	 * SAPI has one, since PHP checks it for NULL. */
	phasync_v_ub_write_prev = sapi_module.ub_write;
	sapi_module.ub_write = phasync_v_ub_write;
	phasync_v_send_headers_prev = sapi_module.send_headers;
	sapi_module.send_headers = phasync_v_send_headers;
	phasync_v_flush_prev = sapi_module.flush;
	sapi_module.flush = phasync_v_flush;
	phasync_v_read_post_prev = sapi_module.read_post;
	sapi_module.read_post = phasync_v_read_post;
	phasync_v_register_server_variables_prev = sapi_module.register_server_variables;
	sapi_module.register_server_variables = phasync_v_register_server_variables;
	for (int i = 0; i < PHASYNC_NSG; i++) {
		phasync_sg_names[i] = zend_string_init_interned(phasync_sg_cnames[i], strlen(phasync_sg_cnames[i]), 1);
	}
	if ((phasync_v_header_handler_prev = sapi_module.header_handler)) {
		sapi_module.header_handler = phasync_v_header_handler;
	}
	zend_observer_fiber_init_register(phasync_v_fiber_init);
	zend_observer_fiber_switch_register(phasync_v_fiber_switch);
	zend_observer_fiber_destroy_register(phasync_v_fiber_destroy);
#if PHP_VERSION_ID < 80400
	phasync_exit_opcode_prev = zend_get_user_opcode_handler(ZEND_EXIT);
	zend_set_user_opcode_handler(ZEND_EXIT, phasync_exit_opcode);
#endif
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
	zend_hash_init(&phasync_globals->inflight, 8, NULL, phasync_hook_entry_dtor, 1);
	zend_hash_init(&phasync_globals->ledgers, 8, NULL, phasync_hook_entry_dtor, 1);
	zend_hash_init(&phasync_globals->wrapped_ops_cache, 8, NULL, phasync_ops_dtor, 1);
	zend_hash_init(&phasync_globals->fs_hooks, 32, NULL, NULL, 1);
	zend_hash_init(&phasync_globals->sock_hooks, 8, NULL, NULL, 1);
	zend_hash_init(&phasync_globals->vfibers, 8, NULL, NULL, 1);
	ZVAL_UNDEF(&phasync_globals->preempt_fn);
	phasync_globals->mountinfo_fd = -1;
}

static PHP_GSHUTDOWN_FUNCTION(phasync)
{
	zend_hash_destroy(&phasync_globals->hooked);
	zend_hash_destroy(&phasync_globals->inflight);
	zend_hash_destroy(&phasync_globals->ledgers);
	zend_hash_destroy(&phasync_globals->wrapped_ops_cache);
	zend_hash_destroy(&phasync_globals->fs_hooks);
	zend_hash_destroy(&phasync_globals->sock_hooks);
	zend_hash_destroy(&phasync_globals->vfibers);
	for (int i = 0; i < phasync_globals->nmounts; i++) {
		free(phasync_globals->mounts[i].path);
	}
	free(phasync_globals->mounts);
	free(phasync_globals->nowait_off);
	if (phasync_globals->mountinfo_fd >= 0) {
		close(phasync_globals->mountinfo_fd);
	}
}

static PHP_MSHUTDOWN_FUNCTION(phasync)
{
	phasync_pool_shutdown();
	php_stream_stdio_ops.read = phasync_stdio_read_orig;
	php_stream_stdio_ops.write = phasync_stdio_write_orig;
	php_stream_stdio_ops.set_option = phasync_stdio_set_option_orig;
	php_stream_stdio_ops.close = phasync_stdio_close_orig;
	sapi_module.ub_write = phasync_v_ub_write_prev;
	sapi_module.send_headers = phasync_v_send_headers_prev;
	sapi_module.flush = phasync_v_flush_prev;
	sapi_module.read_post = phasync_v_read_post_prev;
	if (phasync_v_header_handler_prev) {
		sapi_module.header_handler = phasync_v_header_handler_prev;
	}
#if PHP_VERSION_ID < 80400
	zend_set_user_opcode_handler(ZEND_EXIT, phasync_exit_opcode_prev);
#endif
	zend_string_release_ex(phasync_uninterruptible_lcname, 1);
#if PHP_VERSION_ID >= 80400
	{
		zval zv;   /* the checkpoint, kept out of the function table */

		ZVAL_PTR(&zv, phasync_checkpoint_func);
		zend_function_dtor(&zv);
	}
#endif
	if (phasync_ub_write_orig) {
		sapi_module.ub_write = phasync_ub_write_orig;
	}
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
		} else if (include_files && S_ISREG(st.st_mode) && stream->ops == &php_stream_stdio_ops
		        && phasync_fs_fd_offloaded(fd)) {
			phasync_wrap_stream(stream, PHASYNC_MODE_POOL);
		}
	} ZEND_HASH_FOREACH_END();
}

static PHP_RINIT_FUNCTION(phasync)
{
	PHASYNC_G(scope_top) = NULL;
	PHASYNC_G(vb_cur) = NULL;
	PHASYNC_G(vcount) = 0;
	zend_hash_clean(&PHASYNC_G(vfibers));
	PHASYNC_G(spawn) = NULL;
	PHASYNC_G(ub_writing) = false;
	PHASYNC_G(no_suspend) = 0;
	PHASYNC_G(hooks_installed) = 0;
	zend_hash_clean(&PHASYNC_G(hooked));
	zend_hash_clean(&PHASYNC_G(inflight));
	PHASYNC_G(holds) = 0;
	/* Always-on: the transport factories and function overrides go in at the
	 * start of every request (they are inert while no manage() scope is active),
	 * and streams that predate them get wrapped now. */
	phasync_install_hooks();
	phasync_wrap_existing_streams(false);
	/* Per request, so opcache never substitutes it (see set_preempt_function()). */
	zend_register_bool_constant(ZEND_STRL("phasync\\ext\\__PREEMPT_DUE"), false, 0, module_number);
	PHASYNC_G(preempt_due) = &((zend_constant *) zend_hash_find_ptr(EG(zend_constants), phasync_preempt_due_name))->value;
	return SUCCESS;
}

static PHP_RSHUTDOWN_FUNCTION(phasync)
{
	if (PHASYNC_G(preempt)) {
		phasync_preempt_stop(PHASYNC_G(preempt));
		PHASYNC_G(preempt) = NULL;
	}
	zval_ptr_dtor(&PHASYNC_G(preempt_fn));
	ZVAL_UNDEF(&PHASYNC_G(preempt_fn));
	phasync_restore_hooks();
	/* Any manage() scopes have unwound already (their frames live on the C
	 * stack); nothing to free here. Leave the hooked table intact: streams may
	 * close later in shutdown. */
	PHASYNC_G(scope_top) = NULL;
	/* Fibers PHP freed without resuming them (after a fatal error): settle what
	 * the extension still holds for them. */
	{
		phasync_ledger *l;
		ZEND_HASH_FOREACH_PTR(&PHASYNC_G(ledgers), l) {
			phasync_ledger_settle(l);
		} ZEND_HASH_FOREACH_END();
		zend_hash_clean(&PHASYNC_G(ledgers));
	}
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
