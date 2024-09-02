// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#include <DataStreams/IBlockInputStream.h>

namespace AH
{


/// It's safe to access children without mutex as long as these methods are called before first call to `read()` or `readPrefix()`.


Block IBlockInputStream::read()
{
    Block res;
    if (isCancelledOrThrowIfKilled())
        return res;

    return readImpl();
}


void IBlockInputStream::readPrefix()
{
    readPrefixImpl();

    forEachChild([&] (IBlockInputStream & child)
    {
        child.readPrefix();
        return false;
    });
}


void IBlockInputStream::readSuffix()
{
    forEachChild([&] (IBlockInputStream & child)
    {
        child.readSuffix();
        return false;
    });

    readSuffixImpl();
}


void IBlockInputStream::cancel(bool kill)
{
#if 0
    if (kill)
        is_killed = true;
#endif
    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    forEachChild([&] (IBlockInputStream & child)
    {
        child.cancel(kill);
        return false;
    });
}


bool IBlockInputStream::isCancelled() const
{
    return is_cancelled;
}

bool IBlockInputStream::isCancelledOrThrowIfKilled() const
{
    if (!is_cancelled)
        return false;
    return true;
}

}
