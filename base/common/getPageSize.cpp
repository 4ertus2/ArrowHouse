#include <common/getPageSize.h>
#include <unistd.h>
#include <cstdlib>

int64_t getPageSize()
{
    int64_t page_size = sysconf(_SC_PAGESIZE);
    if (page_size < 0)
        abort();
    return page_size;
}
