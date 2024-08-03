#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fmt/format.h>

//#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <common/getPageSize.h>
#include <IO/MMappedFileDescriptor.h>


namespace AH
{

static size_t getFileSize(int fd)
{
    struct stat stat_res {};
    if (0 != fstat(fd, &stat_res))
        throw ErrnoException("MMappedFileDescriptor: Cannot fstat");

    off_t file_size = stat_res.st_size;

    if (file_size < 0)
        throw Exception("MMappedFileDescriptor: fstat returned negative file size");

    return file_size;
}


MMappedFileDescriptor::MMappedFileDescriptor(int fd_, size_t offset_, size_t length_)
{
    set(fd_, offset_, length_);
}

MMappedFileDescriptor::MMappedFileDescriptor(int fd_, size_t offset_)
    : fd(fd_), offset(offset_)
{
    set(fd_, offset_);
}

void MMappedFileDescriptor::set(int fd_, size_t offset_, size_t length_)
{
    finish();

    fd = fd_;
    offset = offset_;
    length = length_;

    if (!length)
        return;

    void * buf = mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset);
    if (MAP_FAILED == buf)
        throw ErrnoException(fmt::format("MMappedFileDescriptor: Cannot mmap {}", length));

    data = static_cast<char *>(buf);

    //files_metric_increment.changeTo(1);
    //bytes_metric_increment.changeTo(length);
}

void MMappedFileDescriptor::set(int fd_, size_t offset_)
{
    size_t file_size = getFileSize(fd_);

    if (offset > file_size)
        throw Exception("MMappedFileDescriptor: requested offset is greater than file size");

    set(fd_, offset_, file_size - offset);
}

void MMappedFileDescriptor::finish()
{
    if (!length)
        return;

    if (0 != munmap(data, length))
        throw ErrnoException(fmt::format("MMappedFileDescriptor: Cannot munmap {}", length));

    length = 0;

    //files_metric_increment.changeTo(0);
    //bytes_metric_increment.changeTo(0);
}

MMappedFileDescriptor::~MMappedFileDescriptor()
{
    finish(); /// Exceptions will lead to std::terminate and that's Ok.
}

}
