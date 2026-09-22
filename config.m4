PHP_ARG_ENABLE([phasync],
  [whether to enable phasync support],
  [AS_HELP_STRING([--enable-phasync], [Enable phasync (growable stream_select + async stream hooks)])],
  [no])

if test "$PHP_PHASYNC" != "no"; then
  AC_DEFINE(HAVE_PHASYNC, 1, [ phasync extension enabled ])
  PHP_NEW_EXTENSION(phasync, phasync.c, $ext_shared)
fi
