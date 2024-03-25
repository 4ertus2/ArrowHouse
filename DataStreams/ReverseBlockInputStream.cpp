#include <DataStreams/ReverseBlockInputStream.h>

namespace CH
{

ReverseBlockInputStream::ReverseBlockInputStream(const BlockInputStreamPtr & input)
{
    children.push_back(input);
}

Block ReverseBlockInputStream::readImpl()
{
    auto block = children.back()->read();

    if (!block || block->num_rows() < 2)
        return block;

    std::shared_ptr<arrow::UInt64Array> permutation;
    {
        size_t rows_size = block->num_rows();

        arrow::UInt64Builder builder;
        builder.Reserve(rows_size).ok();
        for (size_t i = 0; i < rows_size; ++i)
            builder.Append(rows_size - 1 - i).ok();

        builder.Finish(&permutation).ok();
    }

    auto res = arrow::compute::Take(block, permutation);
    return (*res).record_batch();
}

}
