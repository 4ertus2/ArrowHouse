#pragma once
#include "arrow_clickhouse_types.h"

#include <city.h>
#include <wyhash.h>
#include <xxhash.h>

#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Common/transformEndianness.h>

//#include <bit>
//#include <base/range.h>
//#include <base/bit_cast.h>
//#include <base/unaligned.h>

namespace CH
{

struct IntHash32Impl
{
    using ReturnType = UInt32;

    static UInt32 apply(UInt64 x)
    {
        /// seed is taken from /dev/urandom. It allows you to avoid undesirable dependencies with hashes in different data structures.
        return intHash32<0x75D9543DE018BF45ULL>(x);
    }
};

struct IntHash64Impl
{
    using ReturnType = UInt64;

    static UInt64 apply(UInt64 x) { return intHash64(x ^ 0x4CF2D2BAAE6DA887ULL); }
};

template <typename T, typename HashFunction>
T combineHashesFunc(T t1, T t2)
{
    transformEndianness<std::endian::little>(t1);
    transformEndianness<std::endian::little>(t2);
    const T hashes[]{t1, t2};
    return HashFunction::apply(reinterpret_cast<const char *>(hashes), sizeof(hashes));
}

struct ImplCityHash64
{
    static constexpr auto name = "cityHash64";
    using ReturnType = UInt64;
    using uint128_t = CityHash_v1_0_2::uint128;

    static auto combineHashes(UInt64 h1, UInt64 h2) { return CityHash_v1_0_2::Hash128to64(uint128_t(h1, h2)); }
    static auto apply(const char * s, const size_t len) { return CityHash_v1_0_2::CityHash64(s, len); }
    static constexpr bool use_int_hash_for_pods = true;
};

struct ImplXxHash32
{
    static constexpr auto name = "xxHash32";
    using ReturnType = UInt32;

    static auto apply(const char * s, const size_t len) { return XXH_INLINE_XXH32(s, len, 0); }
    /**
      *  With current implementation with more than 1 arguments it will give the results
      *  non-reproducible from outside of CH.
      *
      *  Proper way of combining several input is to use streaming mode of hash function
      *  https://github.com/Cyan4973/xxHash/issues/114#issuecomment-334908566
      *
      *  In common case doable by init_state / update_state / finalize_state
      */
    static auto combineHashes(UInt32 h1, UInt32 h2) { return IntHash32Impl::apply(h1) ^ h2; }

    static constexpr bool use_int_hash_for_pods = false;
};

struct ImplXxHash64
{
    static constexpr auto name = "xxHash64";
    using ReturnType = UInt64;
    using uint128_t = CityHash_v1_0_2::uint128;

    static auto apply(const char * s, const size_t len) { return XXH_INLINE_XXH64(s, len, 0); }

    /*
       With current implementation with more than 1 arguments it will give the results
       non-reproducible from outside of CH. (see comment on ImplXxHash32).
     */
    static auto combineHashes(UInt64 h1, UInt64 h2) { return CityHash_v1_0_2::Hash128to64(uint128_t(h1, h2)); }

    static constexpr bool use_int_hash_for_pods = false;
};

struct ImplXXH3
{
    static constexpr auto name = "xxh3";
    using ReturnType = UInt64;
    using uint128_t = CityHash_v1_0_2::uint128;

    static auto apply(const char * s, const size_t len) { return XXH_INLINE_XXH3_64bits(s, len); }

    /*
       With current implementation with more than 1 arguments it will give the results
       non-reproducible from outside of CH. (see comment on ImplXxHash32).
     */
    static auto combineHashes(UInt64 h1, UInt64 h2) { return CityHash_v1_0_2::Hash128to64(uint128_t(h1, h2)); }

    static constexpr bool use_int_hash_for_pods = false;
};

struct ImplWyHash64
{
    static constexpr auto name = "wyHash64";
    using ReturnType = UInt64;

    static UInt64 apply(const char * s, const size_t len) { return wyhash(s, len, 0, _wyp); }
    static UInt64 combineHashes(UInt64 h1, UInt64 h2) { return combineHashesFunc<UInt64, ImplWyHash64>(h1, h2); }

    static constexpr bool use_int_hash_for_pods = false;
};

}
