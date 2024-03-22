#pragma once
#include <map>
#include <arrow/api.h>
#include "switch_type.h"

namespace CHY
{

struct SortDescription;

inline bool HasNulls(const std::shared_ptr<arrow::Array> & column)
{
    return column->null_bitmap_data();
}

std::shared_ptr<arrow::RecordBatch>
ExtractColumns(const std::shared_ptr<arrow::RecordBatch> & srcBatch, const std::vector<std::string> & columnNames);
std::shared_ptr<arrow::RecordBatch> ExtractColumns(
    const std::shared_ptr<arrow::RecordBatch> & srcBatch, const std::shared_ptr<arrow::Schema> & dstSchema, bool addNotExisted = false);


std::shared_ptr<arrow::RecordBatch> ToBatch(const std::shared_ptr<arrow::Table> & combinedTable, const bool combine = false);
std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const std::shared_ptr<arrow::Field> & field);

std::vector<std::unique_ptr<arrow::ArrayBuilder>>
MakeBuilders(const std::shared_ptr<arrow::Schema> & schema, size_t reserve = 0, const std::map<std::string, uint64_t> & sizeByColumn = {});
std::vector<std::shared_ptr<arrow::Array>> Finish(std::vector<std::unique_ptr<arrow::ArrayBuilder>> && builders);

std::shared_ptr<arrow::UInt64Array> MakeUI64Array(uint64_t value, int64_t size);
bool ReserveData(arrow::ArrayBuilder & builder, const size_t size);

std::shared_ptr<arrow::UInt64Array>
MakeSortPermutation(const std::shared_ptr<arrow::RecordBatch> & batch, const std::shared_ptr<arrow::Schema> & sortingKey);
bool IsTrivialPermutation(const arrow::UInt64Array & permutation);
bool IsSorted(const std::shared_ptr<arrow::RecordBatch> & batch, const std::shared_ptr<arrow::Schema> & sortingKey, bool desc = false);
bool IsSortedAndUnique(
    const std::shared_ptr<arrow::RecordBatch> & batch, const std::shared_ptr<arrow::Schema> & sortingKey, bool desc = false);

// Return size in bytes including size of bitmap mask
uint64_t GetBatchDataSize(const std::shared_ptr<arrow::RecordBatch> & batch);
// Return size in bytes *not* including size of bitmap mask
uint64_t GetArrayDataSize(const std::shared_ptr<arrow::Array> & column);

std::shared_ptr<arrow::RecordBatch> CombineSortedBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>> & batches, const std::shared_ptr<SortDescription> & description);
std::vector<std::shared_ptr<arrow::RecordBatch>> MergeSortedBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>> & batches,
    const std::shared_ptr<SortDescription> & description,
    size_t maxBatchRows);

bool MergeBatchColumns(
    const std::vector<std::shared_ptr<arrow::RecordBatch>> & batches,
    std::shared_ptr<arrow::RecordBatch> & result,
    const std::vector<std::string> & columnsOrder = {},
    const bool orderFieldsAreNecessary = true);
bool MergeBatchColumns(
    const std::vector<std::shared_ptr<arrow::Table>> & batches,
    std::shared_ptr<arrow::Table> & result,
    const std::vector<std::string> & columnsOrder = {},
    const bool orderFieldsAreNecessary = true);

}
