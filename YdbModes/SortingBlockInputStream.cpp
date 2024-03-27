#include <YdbModes/SortingBlockInputStream.h>
#include <YdbModes/helpers.h>


namespace AHY
{

Block SortingBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block || block->num_rows() == 0)
        return block;

    auto permutation = AHY::MakeSortPermutation(block, description);
    if (AHY::IsTrivialPermutation(*permutation))
        return block;

    auto res = arrow::compute::Take(block, permutation);
    if (!res.ok())
        throw std::runtime_error("sort failed: " + res.status().ToString());
    return res->record_batch();
}

}
