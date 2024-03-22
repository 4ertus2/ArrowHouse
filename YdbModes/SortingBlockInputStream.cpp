#include <YdbModes/SortingBlockInputStream.h>
#include <YdbModes/helpers.h>


namespace CHY
{

Block SortingBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block || block->num_rows() == 0)
        return block;

    auto permutation = CHY::MakeSortPermutation(block, description.sorting_key);
    if (CHY::IsTrivialPermutation(*permutation))
        return block;

    auto res = arrow::compute::Take(block, permutation);
    if (!res.ok())
        throw std::runtime_error("sort failed: " + res.status().ToString());
    return res->record_batch();
}

}
