AC_INIT([iqueue], [0.1.0], [sbaugh@twosigma.com])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIRS([m4])
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects])
AC_PROG_CC
AC_PROG_CXX
AM_PROG_AR
LT_INIT
dnl workaround for https://github.com/kimwalisch/primesieve/issues/16
AC_SUBST(AR_FLAGS, [cr])
PKG_INSTALLDIR
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([
 Makefile
 iqueue.pc
])
AC_OUTPUT
