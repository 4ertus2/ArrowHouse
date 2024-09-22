#include <DataStreams/MergeSortingBlockInputStream.h>
#include <YdbModes/MergingSortedInputStream.h>
#include <YdbModes/helpers.h>

namespace AH
{

using MergingSortedBlockInputStream = AHY::MergingSortedInputStream;

MergeSortingBlockInputStream::MergeSortingBlockInputStream(
    const InputStreamPtr & input, const SortDescription & description_, size_t max_merged_block_size_)
    : description(description_), max_merged_block_size(max_merged_block_size_)
{
    children.push_back(input);
    header = IBlockInputStream::getHeader(children.at(0));
}


Block MergeSortingBlockInputStream::readImpl()
{
    /** Algorithm:
      * - read to memory blocks from source stream;
      * - if too many of them and if external sorting is enabled,
      *   - merge all blocks to sorted stream and write it to temporary file;
      * - at the end, merge all sorted streams from temporary files and also from rest of blocks in memory.
      */

    /// If has not read source blocks.
    if (!impl)
    {
        while (Block block = children.back()->read())
        {
            blocks.push_back(block);

            /** If too many of them and if external sorting is enabled,
              *  will merge blocks that we have in memory at this moment and write merged stream to temporary (compressed) file.
              * NOTE. It's possible to check free space in filesystem.
              */
            if (max_bytes_before_external_sort)
            {
                sum_bytes_in_blocks += AHY::GetBatchDataSize(block);
                if (sum_bytes_in_blocks > max_bytes_before_external_sort)
                    throw std::runtime_error("Not implemented");
#if 0
                const std::string tmp_path(reservation->getDisk()->getPath());
                temporary_files.emplace_back(createTemporaryFile(tmp_path));

                const std::string & path = temporary_files.back()->path();
                MergingSortedBlockInputStream block_in(blocks, description, max_merged_block_size);

                TemporaryFileStream::write(path, header, block_in, &is_cancelled);

                blocks.clear();
                sum_bytes_in_blocks = 0;
#endif
            }
        }

        if (blocks.empty() /*&& temporary_files.empty()*/)
            return {};

#if 0
        if (!temporary_files.empty())
        {
            /// If there was temporary files.

            /// Create sorted streams to merge.
            for (const auto & file : temporary_files)
            {
                temporary_inputs.emplace_back(std::make_unique<TemporaryFileStream>(file->path(), header));
                inputs_to_merge.emplace_back(temporary_inputs.back()->block_in);
            }

            /// Rest of blocks in memory.
            if (!blocks.empty())
                inputs_to_merge.emplace_back(
                    std::make_shared<MergingSortedBlockInputStream>(blocks, description, max_merged_block_size));

            /// Will merge that sorted streams.
            impl = std::make_unique<MergingSortedBlockInputStream>(inputs_to_merge, description, max_merged_block_size);
        }
        else
#endif
        impl = std::make_unique<MergingSortedBlockInputStream>(blocks, description, max_merged_block_size);
    }

    return impl->read();
}

}
