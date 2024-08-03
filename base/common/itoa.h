#pragma once

#include <common/extended_types.h>

#define FOR_INTEGER_TYPES(M) \
    M(uint8_t) \
    M(AH::UInt8) \
    M(AH::UInt16) \
    M(AH::UInt32) \
    M(AH::UInt64) \
    M(AH::UInt128) \
    M(AH::UInt256) \
    M(int8_t) \
    M(AH::Int8) \
    M(AH::Int16) \
    M(AH::Int32) \
    M(AH::Int64) \
    M(AH::Int128) \
    M(AH::Int256)

#define INSTANTIATION(T) char * itoa(T i, char * p);
FOR_INTEGER_TYPES(INSTANTIATION)

#if defined(OS_DARWIN)
INSTANTIATION(unsigned long)
INSTANTIATION(long)
#endif

#undef FOR_INTEGER_TYPES
#undef INSTANTIATION
