// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "DataStreams/IBlockInputStream.h"
#include "SortCursor.h"

namespace AHY
{

using namespace AH;

struct IRowsBuffer
{
    virtual bool AddRow(const SortCursor & cursor) = 0;
    virtual void Flush() = 0;
    virtual bool Limit() const = 0;
    virtual bool HasLimit() const = 0;
};

/// Merges several sorted streams into one sorted stream.
class MergingSortedInputStream : public IBlockInputStream
{
public:
    MergingSortedInputStream(
        const std::vector<BlockInputStreamPtr> & inputs,
        std::shared_ptr<ReplaceSortDescription> description,
        size_t maxBatchRows,
        bool slice = false);
    MergingSortedInputStream(const std::vector<Block> & blocks_, const SortDescription & description_, size_t max_merged_block_size_);

    std::shared_ptr<arrow::Schema> getHeader() const override { return header; }

protected:
    std::shared_ptr<arrow::RecordBatch> readImpl() override;

private:
    std::shared_ptr<arrow::Schema> header;
    std::shared_ptr<ReplaceSortDescription> description;
    const uint64_t max_batch_size;
    const bool slice_sources;
    bool first = true;
    bool finished = false;
    uint64_t expected_batch_size = 0; /// May be smaller or equal to max_block_size. To do 'reserve' for columns.
    std::map<std::string, uint64_t> column_size;

    std::vector<std::shared_ptr<arrow::RecordBatch>> source_batches;
    std::shared_ptr<CompositeKey> prev_key;

    std::vector<SortCursorImpl> cursors;
    SortingHeap queue;

    void Init();
    void FetchNextBatch(const SortCursor & current, SortingHeap & queue);
    void Merge(IRowsBuffer & rowsBuffer, SortingHeap & queue);

    template <bool replace, bool limit>
    void MergeImpl(IRowsBuffer & rowsBuffer, SortingHeap & queue);
};

}
