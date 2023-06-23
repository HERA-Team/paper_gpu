// nt_memutils.h
//
// Non-temporal memory utility functions

#ifndef _NT_MEMUTILS_H
#define _NT_MEMUTILS_H

#include "config.h"

#if HAVE_AVX512F_INSTRUCTIONS || HAVE_AVX2_INSTRUCTIONS

// Cache bypass (non-temporal) version of memset(dst, 0,len)
void bzero_nt(void * dst, size_t len);

// Cache bypass (non-temporal) version of memcpy(dst, sec, len)
void memcpy_nt(void *dst, const void *src, size_t len);

#else
#warning "AVX2 instructions not available, non-temporal optimzations disabled"
#define bzero_nt(d,l)    memset(d,0,l)
#define memcpy_nt(d,s,l) memcpy(d,s,l)
#endif // HAVE_AVX512F || HAVE_AVX2
#endif // _NT_MEMUTILS_H
