#pragma once

#include <common/extended_types.h>
#include <common/defines.h>

// NOLINTBEGIN(google-runtime-int)

namespace common
{
    /// Multiply and ignore overflow.
    template <typename T1, typename T2>
    inline auto NO_SANITIZE_UNDEFINED mulIgnoreOverflow(T1 x, T2 y)
    {
        return x * y;
    }

    template <typename T1, typename T2>
    inline auto NO_SANITIZE_UNDEFINED addIgnoreOverflow(T1 x, T2 y)
    {
        return x + y;
    }

    template <typename T1, typename T2>
    inline auto NO_SANITIZE_UNDEFINED subIgnoreOverflow(T1 x, T2 y)
    {
        return x - y;
    }

    template <typename T>
    inline auto NO_SANITIZE_UNDEFINED negateIgnoreOverflow(T x)
    {
        return -x;
    }

    template <typename T>
    inline bool addOverflow(T x, T y, T & res)
    {
        return __builtin_add_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(int x, int y, int & res)
    {
        return __builtin_sadd_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(long x, long y, long & res)
    {
        return __builtin_saddl_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(long long x, long long y, long long & res)
    {
        return __builtin_saddll_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(AH::Int128 x, AH::Int128 y, AH::Int128 & res)
    {
        res = addIgnoreOverflow(x, y);
        return (y > 0 && x > std::numeric_limits<AH::Int128>::max() - y) ||
            (y < 0 && x < std::numeric_limits<AH::Int128>::min() - y);
    }

    template <>
    inline bool addOverflow(AH::UInt128 x, AH::UInt128 y, AH::UInt128 & res)
    {
        res = addIgnoreOverflow(x, y);
        return x > std::numeric_limits<AH::UInt128>::max() - y;
    }

    template <>
    inline bool addOverflow(AH::Int256 x, AH::Int256 y, AH::Int256 & res)
    {
        res = addIgnoreOverflow(x, y);
        return (y > 0 && x > std::numeric_limits<AH::Int256>::max() - y) ||
            (y < 0 && x < std::numeric_limits<AH::Int256>::min() - y);
    }

    template <>
    inline bool addOverflow(AH::UInt256 x, AH::UInt256 y, AH::UInt256 & res)
    {
        res = addIgnoreOverflow(x, y);
        return x > std::numeric_limits<AH::UInt256>::max() - y;
    }

    template <typename T>
    inline bool subOverflow(T x, T y, T & res)
    {
        return __builtin_sub_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(int x, int y, int & res)
    {
        return __builtin_ssub_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(long x, long y, long & res)
    {
        return __builtin_ssubl_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(long long x, long long y, long long & res)
    {
        return __builtin_ssubll_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(AH::Int128 x, AH::Int128 y, AH::Int128 & res)
    {
        res = subIgnoreOverflow(x, y);
        return (y < 0 && x > std::numeric_limits<AH::Int128>::max() + y) ||
            (y > 0 && x < std::numeric_limits<AH::Int128>::min() + y);
    }

    template <>
    inline bool subOverflow(AH::UInt128 x, AH::UInt128 y, AH::UInt128 & res)
    {
        res = subIgnoreOverflow(x, y);
        return x < y;
    }

    template <>
    inline bool subOverflow(AH::Int256 x, AH::Int256 y, AH::Int256 & res)
    {
        res = subIgnoreOverflow(x, y);
        return (y < 0 && x > std::numeric_limits<AH::Int256>::max() + y) ||
            (y > 0 && x < std::numeric_limits<AH::Int256>::min() + y);
    }

    template <>
    inline bool subOverflow(AH::UInt256 x, AH::UInt256 y, AH::UInt256 & res)
    {
        res = subIgnoreOverflow(x, y);
        return x < y;
    }

    template <typename T>
    inline bool mulOverflow(T x, T y, T & res)
    {
        return __builtin_mul_overflow(x, y, &res);
    }

    template <typename T, typename U, typename R>
    inline bool mulOverflow(T x, U y, R & res)
    {
        // not built in type, wide integer
        if constexpr (AH::is_big_int_v<T>  || AH::is_big_int_v<R> || AH::is_big_int_v<U>)
        {
            res = mulIgnoreOverflow<R>(x, y);
            return false;
        }
        else
            return __builtin_mul_overflow(x, y, &res);
    }

    template <>
    inline bool mulOverflow(int x, int y, int & res)
    {
        return __builtin_smul_overflow(x, y, &res);
    }

    template <>
    inline bool mulOverflow(long x, long y, long & res)
    {
        return __builtin_smull_overflow(x, y, &res);
    }

    template <>
    inline bool mulOverflow(long long x, long long y, long long & res)
    {
        return __builtin_smulll_overflow(x, y, &res);
    }

    /// Overflow check is not implemented for big integers.

    template <>
    inline bool mulOverflow(AH::Int128 x, AH::Int128 y, AH::Int128 & res)
    {
        res = mulIgnoreOverflow(x, y);
        return false;
    }

    template <>
    inline bool mulOverflow(AH::Int256 x, AH::Int256 y, AH::Int256 & res)
    {
        res = mulIgnoreOverflow(x, y);
        return false;
    }

    template <>
    inline bool mulOverflow(AH::UInt128 x, AH::UInt128 y, AH::UInt128 & res)
    {
        res = mulIgnoreOverflow(x, y);
        return false;
    }

    template <>
    inline bool mulOverflow(AH::UInt256 x, AH::UInt256 y, AH::UInt256 & res)
    {
        res = mulIgnoreOverflow(x, y);
        return false;
    }
}

// NOLINTEND(google-runtime-int)
