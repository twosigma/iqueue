/*
 *    Copyright 2021 Two Sigma Open Source, LLC
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
#ifndef _TWOSIGMA_H_
#define _TWOSIGMA_H_

/* We want everything */
#if defined(__linux__)
# ifndef _LARGEFILE64_SOURCE
#  define _LARGEFILE64_SOURCE
# endif
# ifndef _FILE_OFFSET_BITS
#  define _FILE_OFFSET_BITS 64
# endif
# ifndef _GNU_SOURCE
#  define _GNU_SOURCE
#  include <features.h>
# endif /* _GNU_SOURCE */

#define TSEXPORT __attribute__((visibility ("default")))

/*
 * Fixup string.h: We want _GNU_SOURCE for the extensions, but that exposes
 * the bad glibc strerror_r() which is not standards compliant
 */
/* But not the bad strerror! */
#define strerror_r __gnu_strerror_r
#include <string.h>
#undef strerror_r
/* We don't do the REDIRECT_NTH here because we might not be included first */
# ifdef __cplusplus
extern "C"
# else
extern
# endif
int __xpg_strerror_r (int __errnum, char *__buf, size_t __buflen)
     __THROW __nonnull ((2));
#define strerror_r __xpg_strerror_r

# ifdef __INTEL_COMPILER
#  include "intel_compiler.h"
# endif
#endif /* __linux__ */

#ifndef __GNUC_PREREQ
#define __GNUC_PREREQ(maj,min) 0
#endif

#include <sys/cdefs.h>

#ifndef _C_LABEL
#define	_C_LABEL(x)	x
#define _C_LABEL_STRING(x)	#x

#define	__strong_alias(alias,sym)	       				\
    __asm(".global " _C_LABEL_STRING(alias) "\n"			\
	    _C_LABEL_STRING(alias) " = " _C_LABEL_STRING(sym));

#define	__weak_alias(alias,sym)						\
    __asm(".weak " _C_LABEL_STRING(alias) "\n"			\
	    _C_LABEL_STRING(alias) " = " _C_LABEL_STRING(sym));

#define	__weak_reference(sym)	__attribute__((__weak__))

#define	__warn_references(sym,msg)					\
    __asm(".section .gnu.warning." _C_LABEL_STRING(sym) "\n\t.ascii \"" msg "\"\n\t.text");
#endif

#ifndef __predict_true
/*
 * GNU C version 2.96 adds explicit branch prediction so that
 * the CPU back-end can hint the processor and also so that
 * code blocks can be reordered such that the predicted path
 * sees a more linear flow, thus improving cache behavior, etc.
 *
 * The following two macros provide us with a way to use this
 * compiler feature.  Use __predict_true() if you expect the expression
 * to evaluate to true, and __predict_false() if you expect the
 * expression to evaluate to false.
 *
 * A few notes about usage:
 *
 *	* Generally, __predict_false() error condition checks (unless
 *	  you have some _strong_ reason to do otherwise, in which case
 *	  document it), and/or __predict_true() `no-error' condition
 *	  checks, assuming you want to optimize for the no-error case.
 *
 *	* Other than that, if you don't know the likelihood of a test
 *	  succeeding from empirical or other `hard' evidence, don't
 *	  make predictions.
 *
 *	* These are meant to be used in places that are run `a lot'.
 *	  It is wasteful to make predictions in code that is run
 *	  seldomly (e.g. at subsystem initialization time) as the
 *	  basic block reordering that this affects can often generate
 *	  larger code.
 */

#define	__predict_true(exp)	__builtin_expect((exp) != 0, 1)
#define	__predict_false(exp)	__builtin_expect((exp) != 0, 0)
#endif

#define likely(exp) __predict_true(exp)
#define unlikely(exp) __predict_false(exp)

#define __PACKED __attribute__((__packed__))
#define __ALIGNED(x) __attribute__((__aligned__(x)))
/*
 * The following provides __arraycount()
 */
#include <bsd/sys/cdefs.h>

#ifdef __cplusplus
#define __USE(var)			static_cast<void>(&(var))
#define __UNCONST_T(type,var)		const_cast<type>(var)
#define __UNVOLATILE_T(type,var)	const_cast<type>(var)
#define __STATIC_CAST(type,var)		static_cast<type>(var)
#define __VOIDP_CAST(type,var)		static_cast<type>(var)
#define __REINTERPRET_CAST(type,var)	reinterpret_cast<type>(var)
#else
#define __USE(var)			(void)&(var)
#define __UNCONST_T(type,var)		((type)(intptr_t)(const void *)(var))
#define __UNVOLATILE_T(type,var)	((type)(intptr_t)(volatile void *)(var))
#define __STATIC_CAST(type,var)		((type)(var))
#define __VOIDP_CAST(type,var)		(var)
#define __REINTERPRET_CAST(type,var)	((type)(var))
#endif

#define fieldsizeof(type, field) sizeof(__REINTERPRET_CAST(type *, NULL)->field)

#define __PUNNED_CAST(type,var) \
    ({ uintptr_t __p = (uintptr_t)(var); __STATIC_CAST(type, __p); })

#define __IGNORE(result) \
    __ignore(__STATIC_CAST(unsigned long, result))

static __inline void
__ignore(unsigned long result) {
    __USE(result);
}

# ifdef __INTEL_COMPILER
#  define PRIGROUP
# else
#  define PRIGROUP "'"
# endif

/**
 * force the compiler to write registers to memory, and not to reorder
 * memory operations around this statement.
 *
 * http://software.intel.com/en-us/forums/threading-on-intel-parallel-architectures/topic/65071/
 */
#define compiler_fence() __asm__ __volatile__ ("" : : : "memory")

/**
 * Force the processor to flush pending writes, and not to reorder instructions
 * around this statement. implies compiler_fence()
 */
/* #define processor_fence() _mm_mfence() */
#define processor_fence() __asm__ __volatile__ ("lock; addq $0,0(%%rsp)" \
	: : : "memory")

/**
 * Support the restrict type qualifier in C++.
 * C++ does not include the C99's "restrict" type qualifier, but gcc
 * and clang support an equivalent "__restrict__" type qualifier in C++.
 */
#ifdef __cplusplus
#define restrict __restrict__
#endif

#endif /* _TWOSIGMA_H_ */
