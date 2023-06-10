// nt_memutils.c
//
// Non-temporal memory utility functions

#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <immintrin.h>

#include "nt_memutils.h"

// Implementations of non-temproal memory utility functions requiring AVX512F
// or AVX2

#if HAVE_AVX512F_INSTRUCTIONS || HAVE_AVX2_INSTRUCTIONS

#if HAVE_AVX512F_INSTRUCTIONS
#define TYPE_NT  __m512i
#define LOAD_NT  _mm512_stream_load_si512
#define STORE_NT _mm512_stream_si512
#define ZERO_NT  _mm512_setzero_si512
#define SIZE_NT  6
#elif HAVE_AVX2_INSTRUCTIONS
#define TYPE_NT  __m256i
#define LOAD_NT  _mm256_stream_load_si256
#define STORE_NT _mm256_stream_si256
#define ZERO_NT  _mm256_setzero_si256
#define SIZE_NT  5
#endif

void
bzero_nt(void * dst, size_t len)
{
    // Create wide zero value
    const TYPE_NT zero = ZERO_NT();

    // Cast dst to TYPE_NT pointer
    TYPE_NT * pwide = (TYPE_NT *)dst;

    // Convert len from 1 byte units to wide units
    len >>= SIZE_NT;

    // While len > 0
    while(len) {
        *pwide++ = zero;
        len--;
    }
}

#define min(a, b) ((a) < (b) ? (a) : (b))
#define ALIGN(ptr, align) (((ptr) + (align) - 1) & ~((align) - 1))

void
memcpy_nt(void *dst, const void *src, size_t len)
{
    char * d = (char*)dst;
    const char * s = (const char*)(src);

    // Fall back to memcpy() if unequallly misaligned
    if((((uintptr_t)d) & 31) != (((uintptr_t)s) & 31))
        memcpy(d, s, len);

    // memcpy unaligned header
    if(((uintptr_t)d) & 31) {
        uintptr_t header_bytes = 32 - (((uintptr_t)d) & 31);
        //assert(header_bytes < 32);

        memcpy(d, s, min(header_bytes, len));

        d = (char *)ALIGN((uintptr_t)d, 32);
        s = (char *)ALIGN((uintptr_t)s, 32);
        len -= min(header_bytes, len);
    }

    const int sz = (1<<(SIZE_NT));

    for(; len > sz; s+=sz, d+=sz, len-=sz) {
        // Cast d and s to pointer to wide type
        TYPE_NT *dstwide = (TYPE_NT *)d;
        TYPE_NT *srcwide = (TYPE_NT *)s;

        // Might be better to do load(s),load(s+1)
        // and then store(d),store(d+1)?
        STORE_NT(dstwide++, LOAD_NT(srcwide++));
    }

    // memcpy unaligned trailer (if any)
    if (len > 0)
        memcpy(d, s, len);
}

#endif // HAVE_AVX512F || HAVE_AVX2
