#include <DataStreams/CheckSortedBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <YdbModes/helpers.h>
#include <gtest/gtest.h>

using namespace CH;

static std::shared_ptr<arrow::RecordBatch> TestBatch(int num_rows = 10, std::string column_name = "int64")
{
    auto column = NArrow::MakeUI64Array(42, num_rows);
    std::vector<std::shared_ptr<arrow::Field>> fields = {std::make_shared<arrow::Field>(column_name, column->type())};
    auto schema = std::make_shared<arrow::Schema>(fields);

    // Echo the totals to the client
    auto ret = arrow::RecordBatch::Make(schema, column->length(), {column});
    EXPECT_TRUE(!!ret);
    return ret;
}

TEST(OneBlockInputStream, StreamSmoke)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch();
    OneBlockInputStream stream(batch);
    EXPECT_EQ(stream.getName(), "One");
    batch = stream.read();
}

TEST(ConcatBlockInputStream, StreamSmoke)
{
    std::shared_ptr<arrow::RecordBatch> src_batch = TestBatch();
    BlockInputStreams streams
        = {std::make_shared<OneBlockInputStream>(src_batch),
           std::make_shared<OneBlockInputStream>(src_batch),
           std::make_shared<OneBlockInputStream>(src_batch)};

    auto concat = std::make_shared<ConcatBlockInputStream>(streams);
    EXPECT_EQ(concat->getName(), "Concat");

    for (size_t i = 0; i < streams.size(); ++i)
    {
        auto batch = concat->read();
        EXPECT_EQ(batch.get(), src_batch.get());
    }
}

TEST(CheckSortedBlockInputStream, StreamSmoke)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch();
    auto one = std::make_shared<OneBlockInputStream>(batch);

    SortDescription sort_descr;
    sort_descr.sorting_key = batch->schema();
    sort_descr.replace_key = batch->schema();
    sort_descr.directions = {1};

    auto check = std::make_shared<CheckSortedBlockInputStream>(one, sort_descr);
    check->read();

    sort_descr.directions = {-1};
    check = std::make_shared<CheckSortedBlockInputStream>(one, sort_descr);
    check->read();
}

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}