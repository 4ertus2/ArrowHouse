// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#include "MergingSortedInputStream.h"
#include "helpers.h"
#include "switch_type.h"
#include <queue>

namespace CHY
{

class RowsBuffer : public IRowsBuffer
{
public:
    using Builders = std::vector<std::unique_ptr<arrow::ArrayBuilder>>;

    static constexpr const size_t BUFFER_SIZE = 256;

    RowsBuffer(Builders & columns, size_t maxRows) : Columns(columns), MaxRows(maxRows) { Rows.reserve(BUFFER_SIZE); }

    bool AddRow(const SortCursor & cursor) override
    {
        Rows.emplace_back(cursor->all_columns, cursor->getRow());
        if (Rows.size() >= BUFFER_SIZE)
            Flush();
        ++AddedRows;
        return true;
    }

    void Flush() override
    {
        if (Rows.empty())
            return;
        for (size_t i = 0; i < Columns.size(); ++i)
        {
            arrow::ArrayBuilder & builder = *Columns[i];
            for (auto & [srcColumn, rowPosition] : Rows)
                CHY::Append(builder, *srcColumn->at(i), rowPosition);
        }
        Rows.clear();
    }

    bool Limit() const override { return MaxRows && (AddedRows >= MaxRows); }

    bool HasLimit() const override { return MaxRows; }

private:
    Builders & Columns;
    std::vector<std::pair<const ArrayVec *, size_t>> Rows;
    size_t MaxRows = 0;
    size_t AddedRows = 0;
};

class SlicedRowsBuffer : public IRowsBuffer
{
public:
    SlicedRowsBuffer(size_t maxRows) : MaxRows(maxRows) { }

    bool AddRow(const SortCursor & cursor) override
    {
        if (!Batch)
        {
            Batch = cursor->current_batch;
            Offset = cursor->getRow();
        }
        if (Batch.get() != cursor->current_batch.get())
        {
            // append from another batch
            return false;
        }
        else if (cursor->getRow() != (Offset + AddedRows))
        {
            // append from the same batch with data hole
            return false;
        }
        ++AddedRows;
        return true;
    }

    void Flush() override { }

    bool Limit() const override { return MaxRows && (AddedRows >= MaxRows); }

    bool HasLimit() const override { return MaxRows; }

    std::shared_ptr<arrow::RecordBatch> GetBatch()
    {
        if (Batch)
            return Batch->Slice(Offset, AddedRows);
        return {};
    }

private:
    std::shared_ptr<arrow::RecordBatch> Batch;
    size_t Offset = 0;
    size_t MaxRows = 0;
    size_t AddedRows = 0;
};

MergingSortedInputStream::MergingSortedInputStream(
    const std::vector<BlockInputStreamPtr> & inputs, std::shared_ptr<SortDescription> description_, size_t max_batch_rows_, bool slice)
    : description(description_)
    , max_batch_size(max_batch_rows_)
    , slice_sources(slice)
    , source_batches(inputs.size())
    , cursors(inputs.size())
{
    children.insert(children.end(), inputs.begin(), inputs.end());
    header = children.at(0)->getHeader();
}

/// Read the first blocks, initialize the queue.
void MergingSortedInputStream::Init()
{
    first = false;
    uint64_t total_rows = 0;
    for (size_t i = 0; i < source_batches.size(); ++i)
    {
        auto & batch = source_batches[i];
        if (batch)
            continue;

        batch = children[i]->read();
        if (!batch || batch->num_rows() == 0)
            continue;

        for (int32_t j = 0; j < batch->num_columns(); ++j)
            column_size[batch->column_name(j)] += CHY::GetArrayDataSize(batch->column(j));

        total_rows += batch->num_rows();
        cursors[i] = SortCursorImpl(batch, description, i);
    }

    expected_batch_size = max_batch_size ? std::min(total_rows, max_batch_size) : total_rows;
    if (max_batch_size && max_batch_size < total_rows)
        column_size.clear();

    queue = SortingHeap(cursors, description->not_null);
}

std::shared_ptr<arrow::RecordBatch> MergingSortedInputStream::readImpl()
{
    if (finished)
        return {};

    if (children.size() == 1 && !description->Replace())
        return children[0]->read();

    if (first)
        Init();

    if (slice_sources)
    {
        SlicedRowsBuffer rows_buffer(max_batch_size);
        Merge(rows_buffer, queue);
        auto batch = rows_buffer.GetBatch();
        if (!batch->num_rows())
            return {};
        return batch;
    }
    else
    {
        auto builders = CHY::MakeBuilders(header, expected_batch_size, column_size);
        if (builders.empty())
            return {};

        RowsBuffer rows_buffer(builders, max_batch_size);
        Merge(rows_buffer, queue);

        auto arrays = CHY::Finish(std::move(builders));
        if (!arrays[0]->length())
            return {};
        return arrow::RecordBatch::Make(header, arrays[0]->length(), arrays);
    }
}

/// Get the next block from the corresponding source, if there is one.
void MergingSortedInputStream::FetchNextBatch(const SortCursor & current, SortingHeap & queue)
{
    size_t order = current->order;

    while (true)
    {
        source_batches[order] = children[order]->read();
        auto & batch = source_batches[order];

        if (!batch)
        {
            queue.RemoveTop();
            break;
        }

        if (batch->num_rows())
        {
            cursors[order].Reset(batch);
            queue.ReplaceTop(SortCursor(&cursors[order], description->not_null));
            break;
        }
    }
}

/// Take rows in required order and put them into `rowBuffer`,
/// while the number of rows are no more than `max_block_size`
template <bool replace, bool limit>
void MergingSortedInputStream::MergeImpl(IRowsBuffer & rows_buffer, SortingHeap & queue)
{
    if constexpr (replace)
    {
        if (!prev_key && queue.IsValid())
        {
            auto current = queue.Current();
            prev_key = std::make_shared<ReplaceKey>(current->replace_columns, current->getRow());
            if (!rows_buffer.AddRow(current))
                return;
            // Do not get Next() for simplicity. Lead to a dup
        }
    }

    while (queue.IsValid())
    {
        if constexpr (limit)
        {
            if (rows_buffer.Limit())
                return;
        }

        auto current = queue.Current();

        if constexpr (replace)
        {
            ReplaceKey key(current->replace_columns, current->getRow());

            if (key == *prev_key)
            {
                // do nothing
            }
            else if (rows_buffer.AddRow(current))
            {
                *prev_key = key;
            }
            else
            {
                return;
            }
        }
        else
        {
            if (!rows_buffer.AddRow(current))
                return;
        }

        if (!current->isLast())
        {
            queue.Next();
        }
        else
        {
            rows_buffer.Flush();
            FetchNextBatch(current, queue);
        }
    }

    /// We have read all data. Ask children to cancel providing more data.
    cancel();
    finished = true;
}

void MergingSortedInputStream::Merge(IRowsBuffer & rows_buffer, SortingHeap & queue)
{
    const bool replace = description->Replace();
    const bool limit = rows_buffer.HasLimit();

    if (replace)
        if (limit)
            MergeImpl<true, true>(rows_buffer, queue);
        else
            MergeImpl<true, false>(rows_buffer, queue);
    else if (limit)
        MergeImpl<false, true>(rows_buffer, queue);
    else
        MergeImpl<false, false>(rows_buffer, queue);

    rows_buffer.Flush();
}

}
