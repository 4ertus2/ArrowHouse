// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#include <DataStreams/IBlockInputStream.h>

namespace AH
{


/// It's safe to access children without mutex as long as these methods are called before first call to `read()` or `readPrefix()`.

template <typename T>
std::shared_ptr<T> IInputStream<T>::read()
{
    if (isCancelledOrThrowIfKilled())
        return {};

    return readImpl();
}


template <typename T>
void IInputStream<T>::readPrefix()
{
    readPrefixImpl();

    forEachChild([&] (IInputStream & child)
    {
        child.readPrefix();
        return false;
    });
}


template <typename T>
void IInputStream<T>::readSuffix()
{
    forEachChild([&] (IInputStream & child)
    {
        child.readSuffix();
        return false;
    });

    readSuffixImpl();
}


template <typename T>
void IInputStream<T>::cancel(bool kill)
{
#if 0
    if (kill)
        is_killed = true;
#endif
    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    forEachChild([&] (IInputStream & child)
    {
        child.cancel(kill);
        return false;
    });
}


template <typename T>
bool IInputStream<T>::isCancelled() const
{
    return is_cancelled;
}


template <typename T>
bool IInputStream<T>::isCancelledOrThrowIfKilled() const
{
    if (!is_cancelled)
        return false;
    return true;
}


template class IInputStream<arrow::RecordBatch>;

}
