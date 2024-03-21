#include <DataStreams/CheckSortedBlockInputStream.h>
#include <YdbModes/helpers.h>

namespace CH
{

template <typename T, typename U>
static std::partial_ordering compare(const T & left, const U & right, const SortDescription & sort_description)
{
    for (size_t i = 0; i < sort_description.directions.size(); ++i)
    {
        std::partial_ordering cmp = left.CompareColumnValue(i, right, i);
        if (std::is_neq(cmp)) {
            if (sort_description.directions[i] < 0) {
                return std::is_lt(cmp) ? std::partial_ordering::greater : std::partial_ordering::less;
            }
            return cmp;
        }
    }
    return std::partial_ordering::equivalent;
}

CheckSortedBlockInputStream::CheckSortedBlockInputStream(const BlockInputStreamPtr & input_, const SortDescription & sort_description_)
    : header(input_->getHeader()), sort_description(sort_description_)
{
    children.push_back(input_);
}

Block CheckSortedBlockInputStream::readImpl()
{
    static constexpr const char * err_msg = "Sort order of blocks violated";

    Block block = children.back()->read();
    if (!block || block->num_rows() == 0)
        return block;

    size_t rows = block->num_rows();
    auto sort_block = NArrow::ExtractColumns(block, sort_description.sorting_key);
    auto & columns = sort_block->columns();

    if (last_row && std::is_gt(compare(last_row->ToRaw(), RawReplaceKey(&columns, 0), sort_description)))
        throw std::runtime_error(err_msg);

    for (size_t i = 1; i < rows; ++i)
    {
        auto cmp = compare(RawReplaceKey(&columns, i - 1), RawReplaceKey(&columns, i), sort_description);
        if (std::is_gt(cmp))
            throw std::runtime_error(err_msg);
    }

    last_row = ReplaceKey::FromBatch(sort_block, rows - 1);
    return block;
}

}
