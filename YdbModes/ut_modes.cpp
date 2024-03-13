#include "DataStreams/IBlockStream_fwd.h"
#include "DataStreams/OneBlockInputStream.h"
#include "MergingSortedInputStream.h"
#include "helpers.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <gtest/gtest.h>
#include <unordered_map>

using namespace CH;

namespace
{

struct DataRow
{
    bool Bool;
    int8_t Int8;
    int16_t Int16;
    int32_t Int32;
    int64_t Int64;
    uint8_t UInt8;
    uint16_t UInt16;
    uint32_t UInt32;
    uint64_t UInt64;
    float Float32;
    double Float64;
    std::string String;
    std::string Utf8;
    std::string Json;
    std::string Yson;
    uint16_t Date;
    uint32_t Datetime;
    int64_t Timestamp;
    int64_t Interval;
    std::string JsonDocument;
    //uint64_t Decimal[2];

    bool operator==(const DataRow & r) const
    {
        return (Bool == r.Bool) && (Int8 == r.Int8) && (Int16 == r.Int16) && (Int32 == r.Int32) && (Int64 == r.Int64) && (UInt8 == r.UInt8)
            && (UInt16 == r.UInt16) && (UInt32 == r.UInt32) && (UInt64 == r.UInt64) && (Float32 == r.Float32) && (Float64 == r.Float64)
            && (String == r.String) && (Utf8 == r.Utf8) && (Json == r.Json) && (Yson == r.Yson) && (Date == r.Date)
            && (Datetime == r.Datetime) && (Timestamp == r.Timestamp) && (Interval == r.Interval) && (JsonDocument == r.JsonDocument);
        //(Decimal[0] == r.Decimal[0] && Decimal[1] == r.Decimal[1]);
    }

    static std::shared_ptr<arrow::Schema> Schema()
    {
        std::vector<std::shared_ptr<arrow::Field>> fields = {
            arrow::field("bool", arrow::boolean()),
            arrow::field("i8", arrow::int8()),
            arrow::field("i16", arrow::int16()),
            arrow::field("i32", arrow::int32()),
            arrow::field("i64", arrow::int64()),
            arrow::field("ui8", arrow::uint8()),
            arrow::field("ui16", arrow::uint16()),
            arrow::field("ui32", arrow::uint32()),
            arrow::field("ui64", arrow::uint64()),
            arrow::field("f32", arrow::float32()),
            arrow::field("f64", arrow::float64()),
            arrow::field("string", arrow::binary()),
            arrow::field("utf8", arrow::utf8()),
            arrow::field("json", arrow::utf8()),
            arrow::field("yson", arrow::binary()),
            arrow::field("date", arrow::uint16()),
            arrow::field("datetime", arrow::uint32()),
            arrow::field("ts", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO)),
            arrow::field("ival", arrow::duration(arrow::TimeUnit::TimeUnit::MICRO)),
            arrow::field("json_doc", arrow::binary()),
            //arrow::field("dec", arrow::decimal(NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE)),
        };

        return std::make_shared<arrow::Schema>(std::move(fields));
    }
};

template <typename T>
std::vector<DataRow> ToVector(const std::shared_ptr<T> & table)
{
    std::vector<DataRow> rows;

    auto arbool = std::static_pointer_cast<arrow::BooleanArray>(GetColumn(*table, 0));
    auto ari8 = std::static_pointer_cast<arrow::Int8Array>(GetColumn(*table, 1));
    auto ari16 = std::static_pointer_cast<arrow::Int16Array>(GetColumn(*table, 2));
    auto ari32 = std::static_pointer_cast<arrow::Int32Array>(GetColumn(*table, 3));
    auto ari64 = std::static_pointer_cast<arrow::Int64Array>(GetColumn(*table, 4));
    auto aru8 = std::static_pointer_cast<arrow::UInt8Array>(GetColumn(*table, 5));
    auto aru16 = std::static_pointer_cast<arrow::UInt16Array>(GetColumn(*table, 6));
    auto aru32 = std::static_pointer_cast<arrow::UInt32Array>(GetColumn(*table, 7));
    auto aru64 = std::static_pointer_cast<arrow::UInt64Array>(GetColumn(*table, 8));
    auto arf32 = std::static_pointer_cast<arrow::FloatArray>(GetColumn(*table, 9));
    auto arf64 = std::static_pointer_cast<arrow::DoubleArray>(GetColumn(*table, 10));

    auto arstr = std::static_pointer_cast<arrow::BinaryArray>(GetColumn(*table, 11));
    auto arutf = std::static_pointer_cast<arrow::StringArray>(GetColumn(*table, 12));
    auto arj = std::static_pointer_cast<arrow::BinaryArray>(GetColumn(*table, 13));
    auto ary = std::static_pointer_cast<arrow::BinaryArray>(GetColumn(*table, 14));

    auto ard = std::static_pointer_cast<arrow::UInt16Array>(GetColumn(*table, 15));
    auto ardt = std::static_pointer_cast<arrow::UInt32Array>(GetColumn(*table, 16));
    auto arts = std::static_pointer_cast<arrow::TimestampArray>(GetColumn(*table, 17));
    auto arival = std::static_pointer_cast<arrow::DurationArray>(GetColumn(*table, 18));

    auto arjd = std::static_pointer_cast<arrow::BinaryArray>(GetColumn(*table, 19));
    //auto ardec = std::static_pointer_cast<arrow::Decimal128Array>(GetColumn(*table, 19));

    for (int64_t i = 0; i < table->num_rows(); ++i)
    {
        //uint64_t dec[2];
        //memcpy(dec, ardec->Value(i), 16);
        DataRow r{
            arbool->Value(i),    ari8->Value(i),    ari16->Value(i),   ari32->Value(i), ari64->Value(i), aru8->Value(i),
            aru16->Value(i),     aru32->Value(i),   aru64->Value(i),   arf32->Value(i), arf64->Value(i), arstr->GetString(i),
            arutf->GetString(i), arj->GetString(i), ary->GetString(i), ard->Value(i),   ardt->Value(i),  arts->Value(i),
            arival->Value(i),    arjd->GetString(i)
            //{dec[0], dec[1]}
        };
        rows.emplace_back(std::move(r));
    }

    return rows;
}

class DataRowTableBuilder
{
public:
    DataRowTableBuilder()
        : Bts(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool())
        , Bival(arrow::duration(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool())
    //, Bdec(arrow::decimal(NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE), arrow::default_memory_pool())
    {
    }

    void AddRow(const DataRow & row)
    {
        EXPECT_TRUE(Bbool.Append(row.Bool).ok());
        EXPECT_TRUE(Bi8.Append(row.Int8).ok());
        EXPECT_TRUE(Bi16.Append(row.Int16).ok());
        EXPECT_TRUE(Bi32.Append(row.Int32).ok());
        EXPECT_TRUE(Bi64.Append(row.Int64).ok());
        EXPECT_TRUE(Bu8.Append(row.UInt8).ok());
        EXPECT_TRUE(Bu16.Append(row.UInt16).ok());
        EXPECT_TRUE(Bu32.Append(row.UInt32).ok());
        EXPECT_TRUE(Bu64.Append(row.UInt64).ok());
        EXPECT_TRUE(Bf32.Append(row.Float32).ok());
        EXPECT_TRUE(Bf64.Append(row.Float64).ok());

        EXPECT_TRUE(Bstr.Append(row.String).ok());
        EXPECT_TRUE(Butf.Append(row.Utf8).ok());
        EXPECT_TRUE(Bj.Append(row.Json).ok());
        EXPECT_TRUE(By.Append(row.Yson).ok());

        EXPECT_TRUE(Bd.Append(row.Date).ok());
        EXPECT_TRUE(Bdt.Append(row.Datetime).ok());
        EXPECT_TRUE(Bts.Append(row.Timestamp).ok());
        EXPECT_TRUE(Bival.Append(row.Interval).ok());

        EXPECT_TRUE(Bjd.Append(row.JsonDocument).ok());
        //EXPECT_TRUE(Bdec.Append((const char *)&row.Decimal).ok());
    }

    std::shared_ptr<arrow::Table> Finish()
    {
        std::shared_ptr<arrow::BooleanArray> arbool;
        std::shared_ptr<arrow::Int8Array> ari8;
        std::shared_ptr<arrow::Int16Array> ari16;
        std::shared_ptr<arrow::Int32Array> ari32;
        std::shared_ptr<arrow::Int64Array> ari64;
        std::shared_ptr<arrow::UInt8Array> aru8;
        std::shared_ptr<arrow::UInt16Array> aru16;
        std::shared_ptr<arrow::UInt32Array> aru32;
        std::shared_ptr<arrow::UInt64Array> aru64;
        std::shared_ptr<arrow::FloatArray> arf32;
        std::shared_ptr<arrow::DoubleArray> arf64;

        std::shared_ptr<arrow::BinaryArray> arstr;
        std::shared_ptr<arrow::StringArray> arutf;
        std::shared_ptr<arrow::BinaryArray> arj;
        std::shared_ptr<arrow::BinaryArray> ary;

        std::shared_ptr<arrow::UInt16Array> ard;
        std::shared_ptr<arrow::UInt32Array> ardt;
        std::shared_ptr<arrow::TimestampArray> arts;
        std::shared_ptr<arrow::DurationArray> arival;

        std::shared_ptr<arrow::BinaryArray> arjd;
        //std::shared_ptr<arrow::Decimal128Array> ardec;

        EXPECT_TRUE(Bbool.Finish(&arbool).ok());
        EXPECT_TRUE(Bi8.Finish(&ari8).ok());
        EXPECT_TRUE(Bi16.Finish(&ari16).ok());
        EXPECT_TRUE(Bi32.Finish(&ari32).ok());
        EXPECT_TRUE(Bi64.Finish(&ari64).ok());
        EXPECT_TRUE(Bu8.Finish(&aru8).ok());
        EXPECT_TRUE(Bu16.Finish(&aru16).ok());
        EXPECT_TRUE(Bu32.Finish(&aru32).ok());
        EXPECT_TRUE(Bu64.Finish(&aru64).ok());
        EXPECT_TRUE(Bf32.Finish(&arf32).ok());
        EXPECT_TRUE(Bf64.Finish(&arf64).ok());

        EXPECT_TRUE(Bstr.Finish(&arstr).ok());
        EXPECT_TRUE(Butf.Finish(&arutf).ok());
        EXPECT_TRUE(Bj.Finish(&arj).ok());
        EXPECT_TRUE(By.Finish(&ary).ok());

        EXPECT_TRUE(Bd.Finish(&ard).ok());
        EXPECT_TRUE(Bdt.Finish(&ardt).ok());
        EXPECT_TRUE(Bts.Finish(&arts).ok());
        EXPECT_TRUE(Bival.Finish(&arival).ok());

        EXPECT_TRUE(Bjd.Finish(&arjd).ok());
        //EXPECT_TRUE(Bdec.Finish(&ardec).ok());

        std::shared_ptr<arrow::Schema> schema = DataRow::Schema();
        return arrow::Table::Make(
            schema,
            {
                arbool, ari8,  ari16, ari32, ari64, aru8, aru16, aru32,  aru64, arf32, arf64,
                arstr,  arutf, arj,   ary,   ard,   ardt, arts,  arival, arjd
                //ardec
            });
    }

    static std::shared_ptr<arrow::Table> Build(const std::vector<struct DataRow> & rows)
    {
        DataRowTableBuilder builder;
        for (const DataRow & row : rows)
            builder.AddRow(row);
        return builder.Finish();
    }

private:
    arrow::BooleanBuilder Bbool;
    arrow::Int8Builder Bi8;
    arrow::Int16Builder Bi16;
    arrow::Int32Builder Bi32;
    arrow::Int64Builder Bi64;
    arrow::UInt8Builder Bu8;
    arrow::UInt16Builder Bu16;
    arrow::UInt32Builder Bu32;
    arrow::UInt64Builder Bu64;
    arrow::FloatBuilder Bf32;
    arrow::DoubleBuilder Bf64;
    arrow::BinaryBuilder Bstr;
    arrow::StringBuilder Butf;
    arrow::BinaryBuilder Bj;
    arrow::BinaryBuilder By;
    arrow::UInt16Builder Bd;
    arrow::UInt32Builder Bdt;
    arrow::TimestampBuilder Bts;
    arrow::DurationBuilder Bival;
    arrow::BinaryBuilder Bjd;
    //arrow::Decimal128Builder Bdec;
};

std::shared_ptr<arrow::Table> MakeTable1000()
{
    DataRowTableBuilder builder;

    for (int i = 0; i < 1000; ++i)
    {
        int8_t a = i / 100;
        int16_t b = (i % 100) / 10;
        int32_t c = i % 10;
        builder.AddRow(DataRow{false, a, b, c, i, 1, 1, 1, 1, 1.0f, 1.0, "", "", "", "", 0, 0, 0, 0, {0, 0}});
    }

    auto table = builder.Finish();
    auto schema = table->schema();
    auto tres
        = table->SelectColumns(std::vector<int>{schema->GetFieldIndex("i8"), schema->GetFieldIndex("i16"), schema->GetFieldIndex("i32")});
    EXPECT_TRUE(tres.ok());
    return *tres;
}

std::shared_ptr<arrow::Table> Shuffle(std::shared_ptr<arrow::Table> table)
{
    std::vector<arrow::UInt64Builder::value_type> shuffle(1000);
    for (int i = 0; i < 1000; ++i)
        shuffle[i] = i;
#if 0 // FIXME
    ShuffleRange(shuffle);
#endif
    arrow::UInt64Builder builder;
    EXPECT_TRUE(builder.AppendValues(&shuffle[0], shuffle.size()).ok());

    std::shared_ptr<arrow::UInt64Array> permutation;
    EXPECT_TRUE(builder.Finish(&permutation).ok());

    auto res = arrow::compute::Take(table, permutation);
    EXPECT_TRUE(res.ok());
    return (*res).table();
}

std::shared_ptr<arrow::RecordBatch> ExtractBatch(std::shared_ptr<arrow::Table> table)
{
    std::shared_ptr<arrow::RecordBatch> batch;

    arrow::TableBatchReader reader(*table);
    auto result = reader.Next();
    batch = *result;
    result = reader.Next();
    return batch;
}

std::shared_ptr<arrow::RecordBatch> AddSnapColumn(const std::shared_ptr<arrow::RecordBatch> & batch, uint64_t snap)
{
    auto snapColumn = NArrow::MakeUI64Array(snap, batch->num_rows());
    auto result = batch->AddColumn(batch->num_columns(), "snap", snapColumn);
    return *result;
}

std::unordered_map<uint64_t, uint32_t> CountValues(const std::shared_ptr<arrow::UInt64Array> & array)
{
    std::unordered_map<uint64_t, uint32_t> out;
    for (int i = 0; i < array->length(); ++i)
    {
        uint64_t val = array->Value(i);
        ++out[val];
    }
    return out;
}

uint32_t RestoreValue(uint32_t a, uint32_t b, uint32_t c)
{
    return uint32_t(a) * 100 + b * 10 + c;
}

uint32_t RestoreOne(const std::shared_ptr<arrow::RecordBatch> & batch, int pos)
{
    auto arrA = std::static_pointer_cast<arrow::Int8Array>(batch->GetColumnByName("i8"));
    auto arrB = std::static_pointer_cast<arrow::Int16Array>(batch->GetColumnByName("i16"));
    auto arrC = std::static_pointer_cast<arrow::Int32Array>(batch->GetColumnByName("i32"));
    return RestoreValue(arrA->Value(pos), arrB->Value(pos), arrC->Value(pos));
}

bool CheckSorted1000(const std::shared_ptr<arrow::RecordBatch> & batch, bool desc = false)
{
    auto arrA = std::static_pointer_cast<arrow::Int8Array>(batch->GetColumnByName("i8"));
    auto arrB = std::static_pointer_cast<arrow::Int16Array>(batch->GetColumnByName("i16"));
    auto arrC = std::static_pointer_cast<arrow::Int32Array>(batch->GetColumnByName("i32"));
    EXPECT_TRUE(arrA->length() == arrB->length() && arrA->length() == arrC->length());

    for (int i = 0; i < arrA->length(); ++i)
    {
        uint32_t value = RestoreValue(arrA->Value(i), arrB->Value(i), arrC->Value(i));
        if (desc)
        {
            if (value != (arrA->length() - uint32_t(i) - 1))
                return false;
        }
        else
        {
            if (value != uint32_t(i))
                return false;
        }
    }

    return true;
}

bool CheckSorted(const std::shared_ptr<arrow::RecordBatch> & batch, bool desc = false)
{
    auto arrA = std::static_pointer_cast<arrow::Int8Array>(batch->GetColumnByName("i8"));
    auto arrB = std::static_pointer_cast<arrow::Int16Array>(batch->GetColumnByName("i16"));
    auto arrC = std::static_pointer_cast<arrow::Int32Array>(batch->GetColumnByName("i32"));
    EXPECT_TRUE(arrA->length() == arrB->length() && arrA->length() == arrC->length());

    uint32_t prev = RestoreValue(arrA->Value(0), arrB->Value(0), arrC->Value(0));

    for (int i = 1; i < arrA->length(); ++i)
    {
        uint32_t value = RestoreValue(arrA->Value(i), arrB->Value(i), arrC->Value(i));
        if ((desc && value > prev) || (!desc && value < prev))
            return false;
        prev = value;
    }

    return true;
}

}

TEST(SortWithCompositeKey, YdbModes)
{
    std::shared_ptr<arrow::Table> table = Shuffle(MakeTable1000());

    // Table sort is not supported yet: we need not chunked arrays in MakeSortPermutation
    std::shared_ptr<arrow::RecordBatch> batch = ExtractBatch(table);

    EXPECT_EQ(batch->num_rows(), 1000);
    EXPECT_TRUE(!CheckSorted1000(batch));

    auto sortPermutation = NArrow::MakeSortPermutation(batch, table->schema());

    auto res = arrow::compute::Take(batch, sortPermutation);
    EXPECT_TRUE(res.ok());
    batch = (*res).record_batch();

    EXPECT_EQ(batch->num_rows(), 1000);
    EXPECT_TRUE(CheckSorted1000(batch));
}

TEST(MergingSortedInputStream, YdbModes)
{
    std::shared_ptr<arrow::RecordBatch> batch = ExtractBatch(MakeTable1000());
    EXPECT_TRUE(CheckSorted1000(batch));

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.push_back(batch->Slice(0, 100)); // 0..100
    batches.push_back(batch->Slice(100, 200)); // 100..300
    batches.push_back(batch->Slice(200, 400)); // 200..600
    batches.push_back(batch->Slice(500, 50)); // 500..550
    batches.push_back(batch->Slice(600, 1)); // 600..601

    auto descr = std::make_shared<CH::SortDescription>(batch->schema());
    descr->not_null = true;

    std::vector<std::shared_ptr<arrow::RecordBatch>> sorted;
    { // maxBatchSize = 500, no limit
        std::vector<BlockInputStreamPtr> streams;
        for (auto & batch : batches)
            streams.push_back(std::make_shared<OneBlockInputStream>(batch));

        BlockInputStreamPtr mergeStream = std::make_shared<CH::MergingSortedInputStream>(streams, descr, 500);

        while (auto batch = mergeStream->read())
            sorted.emplace_back(batch);
    }

    EXPECT_EQ(sorted.size(), 2);
    EXPECT_EQ(sorted[0]->num_rows(), 500);
    EXPECT_EQ(sorted[1]->num_rows(), 251);
    EXPECT_TRUE(CheckSorted(sorted[0]));
    EXPECT_TRUE(CheckSorted(sorted[1]));
    EXPECT_TRUE(NArrow::IsSorted(sorted[0], descr->sorting_key));
    EXPECT_TRUE(NArrow::IsSorted(sorted[1], descr->sorting_key));
    EXPECT_TRUE(RestoreOne(sorted[0], 499) <= RestoreOne(sorted[1], 0));
}

TEST(MergingSortedInputStreamReversed, YdbModes)
{
    std::shared_ptr<arrow::RecordBatch> batch = ExtractBatch(MakeTable1000());
    EXPECT_TRUE(CheckSorted1000(batch));

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.push_back(batch->Slice(0, 100)); // 0..100
    batches.push_back(batch->Slice(100, 200)); // 100..300
    batches.push_back(batch->Slice(200, 400)); // 200..600
    batches.push_back(batch->Slice(500, 50)); // 500..550
    batches.push_back(batch->Slice(600, 1)); // 600..601

    auto descr = std::make_shared<CH::SortDescription>(batch->schema());
    descr->not_null = true;
    descr->Inverse();

    std::vector<std::shared_ptr<arrow::RecordBatch>> sorted;
    { // maxBatchSize = 500, no limit
        std::vector<BlockInputStreamPtr> streams;
        for (auto & batch : batches)
            streams.push_back(std::make_shared<CH::OneBlockInputStream>(batch));

        BlockInputStreamPtr mergeStream = std::make_shared<CH::MergingSortedInputStream>(streams, descr, 500);

        while (auto batch = mergeStream->read())
            sorted.emplace_back(batch);
    }

    EXPECT_EQ(sorted.size(), 2);
    EXPECT_EQ(sorted[0]->num_rows(), 500);
    EXPECT_EQ(sorted[1]->num_rows(), 251);
    EXPECT_TRUE(CheckSorted(sorted[0], true));
    EXPECT_TRUE(CheckSorted(sorted[1], true));
    EXPECT_TRUE(NArrow::IsSorted(sorted[0], descr->sorting_key, true));
    EXPECT_TRUE(NArrow::IsSorted(sorted[1], descr->sorting_key, true));
    EXPECT_TRUE(RestoreOne(sorted[0], 499) >= RestoreOne(sorted[1], 0));
}

TEST(MergingSortedInputStreamReplace, YdbModes)
{
    std::shared_ptr<arrow::RecordBatch> batch = ExtractBatch(MakeTable1000());
    EXPECT_TRUE(CheckSorted1000(batch));

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.push_back(AddSnapColumn(batch->Slice(0, 400), 0));
    batches.push_back(AddSnapColumn(batch->Slice(200, 400), 1));
    batches.push_back(AddSnapColumn(batch->Slice(400, 400), 2));
    batches.push_back(AddSnapColumn(batch->Slice(600, 400), 3));

    auto sortingKey = batches[0]->schema();
    auto replaceKey = batch->schema();

    auto descr = std::make_shared<CH::SortDescription>(sortingKey, replaceKey);
    descr->directions.back() = -1; // greater snapshot first
    descr->not_null = true;

    std::vector<std::shared_ptr<arrow::RecordBatch>> sorted;
    {
        std::vector<BlockInputStreamPtr> streams;
        for (auto & batch : batches)
            streams.push_back(std::make_shared<CH::OneBlockInputStream>(batch));

        BlockInputStreamPtr mergeStream = std::make_shared<CH::MergingSortedInputStream>(streams, descr, 5000);

        while (auto batch = mergeStream->read())
            sorted.emplace_back(batch);
    }

    EXPECT_EQ(sorted.size(), 1);
    EXPECT_EQ(sorted[0]->num_rows(), 1000);
    EXPECT_TRUE(CheckSorted1000(sorted[0]));
    EXPECT_TRUE(NArrow::IsSortedAndUnique(sorted[0], descr->sorting_key));

    auto counts = CountValues(std::static_pointer_cast<arrow::UInt64Array>(sorted[0]->GetColumnByName("snap")));
    EXPECT_EQ(counts[0], 200);
    EXPECT_EQ(counts[1], 200);
    EXPECT_EQ(counts[2], 200);
    EXPECT_EQ(counts[3], 400);
}

TEST(MergingSortedInputStreamReplaceReversed, YdbModes)
{
    std::shared_ptr<arrow::RecordBatch> batch = ExtractBatch(MakeTable1000());
    EXPECT_TRUE(CheckSorted1000(batch));

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.push_back(AddSnapColumn(batch->Slice(0, 400), 0));
    batches.push_back(AddSnapColumn(batch->Slice(200, 400), 1));
    batches.push_back(AddSnapColumn(batch->Slice(400, 400), 2));
    batches.push_back(AddSnapColumn(batch->Slice(600, 400), 3));

    auto sortingKey = batches[0]->schema();
    auto replaceKey = batch->schema();

    auto descr = std::make_shared<CH::SortDescription>(sortingKey, replaceKey);
    descr->directions.back() = 1; // greater snapshot last
    descr->not_null = true;
    descr->Inverse();

    std::vector<std::shared_ptr<arrow::RecordBatch>> sorted;
    {
        std::vector<BlockInputStreamPtr> streams;
        for (auto & batch : batches)
            streams.push_back(std::make_shared<CH::OneBlockInputStream>(batch));

        BlockInputStreamPtr mergeStream = std::make_shared<CH::MergingSortedInputStream>(streams, descr, 5000);

        while (auto batch = mergeStream->read())
            sorted.emplace_back(batch);
    }

    EXPECT_EQ(sorted.size(), 1);
    EXPECT_EQ(sorted[0]->num_rows(), 1000);
    EXPECT_TRUE(CheckSorted1000(sorted[0], true));
    EXPECT_TRUE(NArrow::IsSortedAndUnique(sorted[0], descr->sorting_key, true));

    auto counts = CountValues(std::static_pointer_cast<arrow::UInt64Array>(sorted[0]->GetColumnByName("snap")));
    EXPECT_EQ(counts[0], 200);
    EXPECT_EQ(counts[1], 200);
    EXPECT_EQ(counts[2], 200);
    EXPECT_EQ(counts[3], 400);
}

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
