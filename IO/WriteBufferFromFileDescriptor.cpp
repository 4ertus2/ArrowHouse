#include <unistd.h>
#include <errno.h>
#include <cassert>
#include <sys/types.h>
#include <sys/stat.h>

#include <Common/Exception.h>
//#include <Common/ProfileEvents.h>
//#include <Common/CurrentMetrics.h>
//#include <Common/Stopwatch.h>

#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>


namespace AH
{

void WriteBufferFromFileDescriptor::nextImpl()
{
    if (!offset())
        return;

    //Stopwatch watch;

    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        //ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWrite);

        ssize_t res = 0;
        {
            //CurrentMetrics::Increment metric_increment{CurrentMetrics::Write};
            res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            //ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteFailed);
            throw ErrnoException("Cannot write to file " + getFileName());
        }

        if (res > 0)
            bytes_written += res;
    }

    //ProfileEvents::increment(ProfileEvents::DiskWriteElapsedMicroseconds, watch.elapsedMicroseconds());
    //ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteBytes, bytes_written);
}


/// Name or some description of file.
std::string WriteBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(fd) + ")";
}


WriteBufferFromFileDescriptor::WriteBufferFromFileDescriptor(
    int fd_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFileBase(buf_size, existing_memory, alignment), fd(fd_) {}


WriteBufferFromFileDescriptor::~WriteBufferFromFileDescriptor()
{
    try
    {
        if (fd >= 0)
            next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void WriteBufferFromFileDescriptor::sync()
{
    /// If buffer has pending data - write it.
    next();

    /// Request OS to sync data with storage medium.
    int res = fsync(fd);
    if (-1 == res)
        throw ErrnoException("Cannot fsync " + getFileName());
}


off_t WriteBufferFromFileDescriptor::seek(off_t offset, int whence)
{
    off_t res = lseek(fd, offset, whence);
    if (-1 == res)
        throw ErrnoException("Cannot seek through file " + getFileName());
    return res;
}


void WriteBufferFromFileDescriptor::truncate(off_t length)
{
    int res = ftruncate(fd, length);
    if (-1 == res)
        throw ErrnoException("Cannot truncate file " + getFileName());
}


off_t WriteBufferFromFileDescriptor::size()
{
    struct stat buf;
    int res = fstat(fd, &buf);
    if (-1 == res)
        throw ErrnoException("Cannot execute fstat " + getFileName());
    return buf.st_size;
}

}
