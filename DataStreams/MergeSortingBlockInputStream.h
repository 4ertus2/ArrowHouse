#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Common/SortDescription.h>


namespace AH
{

/** Merges stream of sorted each-separately blocks to sorted as-a-whole stream of blocks.
  */

class MergeSortingBlockInputStream : public IBlockInputStream
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    MergeSortingBlockInputStream(const InputStreamPtr & input, const SortDescription & description_, size_t max_merged_block_size_);

    Header getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    SortDescription description;
    size_t max_merged_block_size;
    size_t max_bytes_before_external_sort = 0;

    std::vector<Block> blocks;
    size_t sum_bytes_in_blocks = 0;
    std::unique_ptr<IBlockInputStream> impl;
    Header header;
    //InputStreams inputs_to_merge;
};

}
