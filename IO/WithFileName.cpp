#include <IO/WithFileName.h>
#include <IO/ReadBufferWrapperBase.h>
//#include <IO/ParallelReadBuffer.h>
//#include <IO/PeekableReadBuffer.h>

namespace AH
{

template <typename T>
static String getFileName(const T & entry)
{
    if (const auto * with_file_name = dynamic_cast<const WithFileName *>(&entry))
        return with_file_name->getFileName();
    return "";
}

String getFileNameFromReadBuffer(const ReadBuffer & in)
{
    if (const auto * wrapper = dynamic_cast<const ReadBufferWrapperBase *>(&in))
        return getFileNameFromReadBuffer(wrapper->getWrappedReadBuffer());
#if 0
    else if (const auto * parallel = dynamic_cast<const ParallelReadBuffer *>(&in))
        return getFileNameFromReadBuffer(parallel->getReadBuffer());
    else if (const auto * peekable = dynamic_cast<const PeekableReadBuffer *>(&in))
        return getFileNameFromReadBuffer(peekable->getSubBuffer());
#endif
    else
        return getFileName(in);
}

String getExceptionEntryWithFileName(const ReadBuffer & in)
{
    auto filename = getFileNameFromReadBuffer(in);

    if (filename.empty())
        return "";

    return fmt::format(": While reading from: {}", filename);
}

}
