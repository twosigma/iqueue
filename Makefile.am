## Process this file with automake to produce Makefile.in
AM_CFLAGS = -g -Og -Wall -Wextra -Werror -pthread
AM_CXXFLAGS = $(AM_CFLAGS) -std=gnu++17
AM_CPPFLAGS = -I$(srcdir)/include -I$(srcdir)/src

# Library
pkgconfig_DATA = iqueue.pc
lib_LTLIBRARIES = libiqueue.la

libiqueue_la_SOURCES = src/iqueue.c src/iqsync.c \
	src/math_utils.h src/math_utils.c \
	src/net_utils.h src/net_utils.c \
	src/proc_utils.h src/proc_utils.c \
	src/io_utils.h src/io_utils.c \
	src/shash.c \
	src/tsgosmacs.h src/tsflexhash.h src/tsflexhash_private.h src/tsflexhash.c \
	src/tslog.h src/tslog.c \
	src/tsassert.h src/tsassert.c \
	src/twosigma.h \
	src/tsclock.h \
	src/tstl.h src/tsdir.h \
	src/try_unix.hh \
	src/tstl.c src/tsdir.c src/getlogstr.c \
	src/container_of.h \
	src/tslock.h src/iqmod_common.h
include_HEADERS = include/iqueue.h include/iqsync.h include/shash.h include/iqueue.hh include/stringer.hh

AM_LDFLAGS = -lbsd
# Programs
bin_PROGRAMS = iqueue iqsync wait_for_heartbeat iqueue_tail_count iqmod_copy iqmod_inplace in2iqueue iqueue2out dump_iqueue

iqueue_SOURCES = src/iqueue-main.c
iqueue_LDADD = libiqueue.la

iqsync_SOURCES = src/iqsync-main.c
iqsync_LDADD = libiqueue.la -ldl

wait_for_heartbeat_SOURCES = src/wait_for_heartbeat.cc
wait_for_heartbeat_LDADD = libiqueue.la

iqueue_tail_count_SOURCES = src/iqueue_tail_count.cc
iqueue_tail_count_LDADD = libiqueue.la

iqmod_copy_SOURCES = src/iqmod_copy-main.c
iqmod_copy_LDADD = libiqueue.la

iqmod_inplace_SOURCES = src/iqmod_inplace-main.c
iqmod_inplace_LDADD = libiqueue.la

in2iqueue_SOURCES = src/in2iqueue.cc
in2iqueue_LDADD = libiqueue.la

iqueue2out_SOURCES = src/iqueue2out.cc
iqueue2out_LDADD = libiqueue.la

dump_iqueue_SOURCES = src/dump_iqueue.cc
dump_iqueue_LDADD = libiqueue.la

# Tests
check_LIBRARIES = libtstest.a
libtstest_a_SOURCES = test/ctest.h test/ctest_main.h test/ctest_resource.c test/ctest_resource.h

check_PROGRAMS = iqueue_reopen_ctest grow_ctest iqueue_try_update_ctest iqueue_writer_ctest \
	iqueue_symlink_ctest iqueue_allocator_ctest unlink_ctest iqmod_ctest
iqueue_reopen_ctest_SOURCES = test/iqueue_reopen_ctest.c
iqueue_reopen_ctest_LDADD = libiqueue.la libtstest.a

grow_ctest_SOURCES = test/grow_ctest.c
grow_ctest_LDADD = libiqueue.la libtstest.a

iqueue_try_update_ctest_SOURCES = test/iqueue_try_update_ctest.c
iqueue_try_update_ctest_LDADD = libiqueue.la libtstest.a

iqueue_writer_ctest_SOURCES = test/iqueue_writer_ctest.c
iqueue_writer_ctest_LDADD = libiqueue.la libtstest.a

iqueue_symlink_ctest_SOURCES = test/iqueue_symlink_ctest.c
iqueue_symlink_ctest_LDADD = libiqueue.la libtstest.a

# Make sure TMPDIR is set to a place with lots of space for this one
iqueue_allocator_ctest_SOURCES = test/iqueue_allocator_ctest.c
iqueue_allocator_ctest_LDADD = libiqueue.la libtstest.a

unlink_ctest_SOURCES = test/unlink_ctest.c
unlink_ctest_LDADD = libiqueue.la libtstest.a

iqmod_ctest_SOURCES = test/iqmod_ctest.c
iqmod_ctest_LDADD = libiqueue.la libtstest.a

TESTS = $(check_PROGRAMS)
