#include <DataStreams/FilterColumnsBlockInputStream.h>
#include <Common/projection.h>

namespace AH
{

Header FilterColumnsBlockInputStream::getHeader() const
{
    Header header = children.back()->getHeader();
    return projection(header, columns_to_save, throw_if_column_not_found);
}

Block FilterColumnsBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block)
        return block;

    return projection(block, columns_to_save, throw_if_column_not_found);
}

}
