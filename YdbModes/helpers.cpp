#include "helpers.h"
#include "DataStreams/IBlockStream_fwd.h"
#include "DataStreams/OneBlockInputStream.h"
#include "MergingSortedInputStream.h"
#include "SortCursor.h"
#include "switch_type.h"

#include <limits>
#include <memory>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include "CompositeKey.h"

namespace AHY
{

using AH::BlockInputStreams;

namespace
{
template <class TStringType>
std::shared_ptr<arrow::RecordBatch>
ExtractColumnsImpl(const std::shared_ptr<arrow::RecordBatch> & srcBatch, const std::vector<TStringType> & columnNames)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(columnNames.size());
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(columnNames.size());

    auto srcSchema = srcBatch->schema();
    for (auto & name : columnNames)
    {
        int pos = srcSchema->GetFieldIndex(name);
        if (pos < 0)
            return {};
        fields.push_back(srcSchema->field(pos));
        columns.push_back(srcBatch->column(pos));
    }

    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), srcBatch->num_rows(), std::move(columns));
}
}

std::shared_ptr<arrow::RecordBatch>
ExtractColumns(const std::shared_ptr<arrow::RecordBatch> & srcBatch, const std::vector<std::string> & columnNames)
{
    return ExtractColumnsImpl(srcBatch, columnNames);
}

std::shared_ptr<arrow::RecordBatch>
ExtractColumns(const std::shared_ptr<arrow::RecordBatch> & srcBatch, const std::shared_ptr<arrow::Schema> & dstSchema, bool addNotExisted)
{
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(dstSchema->num_fields());

    for (auto & field : dstSchema->fields())
    {
        columns.push_back(srcBatch->GetColumnByName(field->name()));
        if (!columns.back())
        {
            if (addNotExisted)
            {
                auto result = arrow::MakeArrayOfNull(field->type(), srcBatch->num_rows());
                if (!result.ok())
                    return nullptr;
                columns.back() = *result;
            }
            else
            {
                return nullptr;
            }
        }
        else
        {
            auto srcField = srcBatch->schema()->GetFieldByName(field->name());
            if (!field->Equals(srcField))
                return nullptr;
        }

        if (!columns.back()->type()->Equals(field->type()))
            return nullptr;
    }

    return arrow::RecordBatch::Make(dstSchema, srcBatch->num_rows(), columns);
}

std::shared_ptr<arrow::RecordBatch> ToBatch(const std::shared_ptr<arrow::Table> & tableExt, const bool combine)
{
    std::shared_ptr<arrow::Table> table;
    if (combine)
    {
        auto res = tableExt->CombineChunks();
        table = *res;
    }
    else
    {
        table = tableExt;
    }
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(table->num_columns());
    for (auto & col : table->columns())
        columns.push_back(col->chunk(0));
    return arrow::RecordBatch::Make(table->schema(), table->num_rows(), columns);
}

// Check if the permutation doesn't reorder anything
bool IsTrivialPermutation(const arrow::UInt64Array & permutation)
{
    for (int64_t i = 0; i < permutation.length(); ++i)
        if (permutation.Value(i) != (uint64_t)i)
            return false;
    return true;
}

template <bool desc, bool uniq>
static bool IsSelfSorted(const std::shared_ptr<arrow::RecordBatch> & batch)
{
    if (batch->num_rows() < 2)
        return true;
    auto & columns = batch->columns();

    for (int i = 1; i < batch->num_rows(); ++i)
    {
        RawCompositeKey prev(&columns, i - 1);
        RawCompositeKey current(&columns, i);
        if constexpr (desc)
        {
            if (prev < current)
                return false;
        }
        else
        {
            if (current < prev)
                return false;
        }
        if constexpr (uniq)
        {
            if (prev == current)
                return false;
        }
    }
    return true;
}

bool IsSorted(const std::shared_ptr<arrow::RecordBatch> & batch, const std::shared_ptr<arrow::Schema> & sortingKey, bool desc)
{
    auto keyBatch = ExtractColumns(batch, sortingKey);
    if (desc)
        return IsSelfSorted<true, false>(keyBatch);
    else
        return IsSelfSorted<false, false>(keyBatch);
}

bool IsSortedAndUnique(const std::shared_ptr<arrow::RecordBatch> & batch, const std::shared_ptr<arrow::Schema> & sortingKey, bool desc)
{
    auto keyBatch = ExtractColumns(batch, sortingKey);
    if (desc)
        return IsSelfSorted<true, true>(keyBatch);
    else
        return IsSelfSorted<false, true>(keyBatch);
}


std::vector<std::unique_ptr<arrow::ArrayBuilder>>
MakeBuilders(const std::shared_ptr<arrow::Schema> & schema, size_t reserve, const std::map<std::string, uint64_t> & sizeByColumn)
{
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    builders.reserve(schema->num_fields());

    for (auto & field : schema->fields())
    {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        Validate(arrow::MakeBuilder(arrow::default_memory_pool(), field->type(), &builder));
#if 0
        if (sizeByColumn.size())
        {
            auto it = sizeByColumn.find(field->name());
            if (it != sizeByColumn.end())
                AFL_VERIFY(AHY::ReserveData(*builder, it->second))("size", it->second)("field", field->name());
        }
#endif
        if (reserve)
            Validate(builder->Reserve(reserve));

        builders.emplace_back(std::move(builder));
    }
    return builders;
}

std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const std::shared_ptr<arrow::Field> & field)
{
    std::unique_ptr<arrow::ArrayBuilder> builder;
    Validate(arrow::MakeBuilder(arrow::default_memory_pool(), field->type(), &builder));
    return builder;
}

std::vector<std::shared_ptr<arrow::Array>> Finish(std::vector<std::unique_ptr<arrow::ArrayBuilder>> && builders)
{
    std::vector<std::shared_ptr<arrow::Array>> out;
    for (auto & builder : builders)
    {
        std::shared_ptr<arrow::Array> array;
        Validate(builder->Finish(&array));
        out.emplace_back(array);
    }
    return out;
}


uint64_t GetBatchDataSize(const std::shared_ptr<arrow::RecordBatch> & batch)
{
    if (!batch)
        return 0;
    uint64_t bytes = 0;
    for (auto & column : batch->columns())
    { // TODO: use column_data() instead of columns()
        bytes += GetArrayDataSize(column);
    }
    return bytes;
}

template <typename TType>
uint64_t GetArrayDataSizeImpl(const std::shared_ptr<arrow::Array> & column)
{
    return sizeof(typename TType::c_type) * column->length();
}

template <>
uint64_t GetArrayDataSizeImpl<arrow::NullType>(const std::shared_ptr<arrow::Array> & column)
{
    return column->length() * 8; // Special value for empty lines
}

template <>
uint64_t GetArrayDataSizeImpl<arrow::StringType>(const std::shared_ptr<arrow::Array> & column)
{
    auto typedColumn = std::static_pointer_cast<arrow::StringArray>(column);
    return typedColumn->total_values_length();
}

template <>
uint64_t GetArrayDataSizeImpl<arrow::LargeStringType>(const std::shared_ptr<arrow::Array> & column)
{
    auto typedColumn = std::static_pointer_cast<arrow::StringArray>(column);
    return typedColumn->total_values_length();
}

template <>
uint64_t GetArrayDataSizeImpl<arrow::BinaryType>(const std::shared_ptr<arrow::Array> & column)
{
    auto typedColumn = std::static_pointer_cast<arrow::BinaryArray>(column);
    return typedColumn->total_values_length();
}

template <>
uint64_t GetArrayDataSizeImpl<arrow::LargeBinaryType>(const std::shared_ptr<arrow::Array> & column)
{
    auto typedColumn = std::static_pointer_cast<arrow::BinaryArray>(column);
    return typedColumn->total_values_length();
}

template <>
uint64_t GetArrayDataSizeImpl<arrow::FixedSizeBinaryType>(const std::shared_ptr<arrow::Array> & column)
{
    auto typedColumn = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(column);
    return typedColumn->byte_width() * typedColumn->length();
}

template <>
uint64_t GetArrayDataSizeImpl<arrow::Decimal128Type>(const std::shared_ptr<arrow::Array> & column)
{
    return sizeof(uint64_t) * 2 * column->length();
}

uint64_t GetArrayDataSize(const std::shared_ptr<arrow::Array> & column)
{
    auto type = column->type();
    uint64_t bytes = 0;
    SwitchTypeWithNull(
        type->id(),
        [&]<typename TType>(TTypeWrapper<TType>)
        {
            bytes = GetArrayDataSizeImpl<TType>(column);
            return true;
        });

    // Add null bit mask overhead if any.
    if (HasNulls(column))
        bytes += column->length() / 8 + 1;

    return bytes;
}


std::shared_ptr<arrow::UInt64Array> MakeUI64Array(uint64_t value, int64_t size)
{
    auto res = arrow::MakeArrayFromScalar(arrow::UInt64Scalar(value), size);
    return std::static_pointer_cast<arrow::UInt64Array>(*res);
}

std::shared_ptr<arrow::UInt64Array>
MakeSortPermutation(const std::shared_ptr<arrow::RecordBatch> & batch, const std::shared_ptr<arrow::Schema> & sortingKey)
{
    auto keyBatch = ExtractColumns(batch, sortingKey);
    auto keyColumns = std::make_shared<AHY::ArrayVec>(keyBatch->columns());
    std::vector<RawCompositeKey> points;
    points.reserve(keyBatch->num_rows());

    for (int i = 0; i < keyBatch->num_rows(); ++i)
        points.push_back(RawCompositeKey(keyColumns.get(), i));

    bool haveNulls = false;
    for (auto & column : *keyColumns)
    {
        if (HasNulls(column))
        {
            haveNulls = true;
            break;
        }
    }

    if (haveNulls)
        std::sort(points.begin(), points.end());
    else
        std::sort(
            points.begin(), points.end(), [](const RawCompositeKey & a, const RawCompositeKey & b) { return std::is_lt(a.CompareNotNull(b)); });

    arrow::UInt64Builder builder;
    Validate(builder.Reserve(points.size()));

    for (auto & point : points)
        Validate(builder.Append(point.GetPosition()));

    std::shared_ptr<arrow::UInt64Array> out;
    Validate(builder.Finish(&out));
    return out;
}

bool ReserveData(arrow::ArrayBuilder & builder, const size_t size)
{
    arrow::Status result = arrow::Status::OK();
    if (builder.type()->id() == arrow::Type::BINARY || builder.type()->id() == arrow::Type::STRING)
    {
        static_assert(
            std::is_convertible_v<arrow::StringBuilder &, arrow::BaseBinaryBuilder<arrow::BinaryType> &>,
            "Expected StringBuilder to be BaseBinaryBuilder<BinaryType>");
        auto & bBuilder = static_cast<arrow::BaseBinaryBuilder<arrow::BinaryType> &>(builder);
        result = bBuilder.ReserveData(size);
    }
    return result.ok();
}

std::shared_ptr<arrow::RecordBatch>
CombineSortedBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>> & batches, const std::shared_ptr<SortDescription> & description)
{
    BlockInputStreams streams;
    for (auto & batch : batches)
        streams.push_back(std::make_shared<AH::OneBlockInputStream>(batch));

    auto mergeStream = std::make_shared<AHY::MergingSortedInputStream>(streams, description, std::numeric_limits<uint64_t>::max());
    std::shared_ptr<arrow::RecordBatch> batch = mergeStream->read();
    return batch;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> MergeSortedBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>> & batches,
    const std::shared_ptr<SortDescription> & description,
    size_t max_batch_rows)
{
    uint64_t num_rows = 0;
    BlockInputStreams streams;
    streams.reserve(batches.size());
    for (auto & batch : batches)
    {
        if (batch->num_rows())
        {
            num_rows += batch->num_rows();
            streams.push_back(std::make_shared<AH::OneBlockInputStream>(batch));
        }
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> out;
    out.reserve(num_rows / max_batch_rows + 1);

    auto merge_stream = std::make_shared<AHY::MergingSortedInputStream>(streams, description, max_batch_rows);
    while (std::shared_ptr<arrow::RecordBatch> batch = merge_stream->read())
        out.push_back(batch);
    return out;
}

template <class TData, class TColumn, class TBuilder>
bool MergeBatchColumnsImpl(
    const std::vector<std::shared_ptr<TData>> & batches,
    std::shared_ptr<TData> & result,
    const std::vector<std::string> & columnsOrder,
    const bool orderFieldsAreNecessary,
    const TBuilder & builder)
{
    if (batches.empty())
    {
        result = nullptr;
        return true;
    }
    if (batches.size() == 1)
    {
        result = batches.front();
        return true;
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<TColumn>> columns;
    std::map<std::string, uint32_t> fieldNames;
    for (auto && i : batches)
    {
        for (auto && f : i->schema()->fields())
        {
            if (!fieldNames.emplace(f->name(), fields.size()).second)
                return false;
            fields.emplace_back(f);
        }
        if (i->num_rows() != batches.front()->num_rows())
            return false;
        for (auto && c : i->columns())
            columns.emplace_back(c);
    }

    if (columnsOrder.size())
    {
        std::vector<std::shared_ptr<arrow::Field>> fieldsOrdered;
        std::vector<std::shared_ptr<TColumn>> columnsOrdered;
        for (auto && i : columnsOrder)
        {
            auto it = fieldNames.find(i);
            if (orderFieldsAreNecessary)
                ;
            else if (it == fieldNames.end())
                continue;
            fieldsOrdered.emplace_back(fields[it->second]);
            columnsOrdered.emplace_back(columns[it->second]);
        }
        std::swap(fieldsOrdered, fields);
        std::swap(columnsOrdered, columns);
    }
    result = builder(std::make_shared<arrow::Schema>(fields), batches.front()->num_rows(), std::move(columns));
    return true;
}

bool MergeBatchColumns(
    const std::vector<std::shared_ptr<arrow::Table>> & batches,
    std::shared_ptr<arrow::Table> & result,
    const std::vector<std::string> & columnsOrder,
    const bool orderFieldsAreNecessary)
{
    const auto builder
        = [](const std::shared_ptr<arrow::Schema> & schema,
             const uint32_t recordsCount,
             std::vector<std::shared_ptr<arrow::ChunkedArray>> && columns) { return arrow::Table::Make(schema, columns, recordsCount); };

    return MergeBatchColumnsImpl<arrow::Table, arrow::ChunkedArray>(batches, result, columnsOrder, orderFieldsAreNecessary, builder);
}

bool MergeBatchColumns(
    const std::vector<std::shared_ptr<arrow::RecordBatch>> & batches,
    std::shared_ptr<arrow::RecordBatch> & result,
    const std::vector<std::string> & columnsOrder,
    const bool orderFieldsAreNecessary)
{
    const auto builder
        = [](const std::shared_ptr<arrow::Schema> & schema,
             const uint32_t recordsCount,
             std::vector<std::shared_ptr<arrow::Array>> && columns) { return arrow::RecordBatch::Make(schema, recordsCount, columns); };

    return MergeBatchColumnsImpl<arrow::RecordBatch, arrow::Array>(batches, result, columnsOrder, orderFieldsAreNecessary, builder);
}

}
