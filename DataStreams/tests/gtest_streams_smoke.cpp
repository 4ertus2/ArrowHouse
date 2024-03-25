#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/FilterColumnsBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/ReverseBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <YdbModes/CheckSortedBlockInputStream.h>
#include <YdbModes/helpers.h>
#include <gtest/gtest.h>

using namespace CH;

static std::shared_ptr<arrow::RecordBatch> TestBatch(int num_rows = 10, std::string column_name = "int64")
{
    auto column = CHY::MakeUI64Array(42, num_rows);
    std::vector<std::shared_ptr<arrow::Field>> fields = {std::make_shared<arrow::Field>(column_name, column->type())};
    auto schema = std::make_shared<arrow::Schema>(fields);

    // Echo the totals to the client
    auto ret = arrow::RecordBatch::Make(schema, column->length(), {column});
    EXPECT_TRUE(!!ret);
    return ret;
}

TEST(NullBlockInputStream, StreamSmoke)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch();
    NullBlockInputStream stream(batch->schema());
    EXPECT_EQ(stream.getName(), "Null");
    EXPECT_EQ(stream.read().get(), nullptr);
}

TEST(OneBlockInputStream, StreamSmoke)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch();
    OneBlockInputStream stream(batch);
    EXPECT_EQ(stream.getName(), "One");
    batch = stream.read();
}

TEST(BlocksListBlockInputStream, StreamSmoke)
{
    std::shared_ptr<arrow::RecordBatch> src_batch = TestBatch();

    auto list = std::make_shared<BlocksListBlockInputStream>(CH::BlocksList{src_batch, src_batch});
    EXPECT_EQ(list->getName(), "BlocksList");

    while (auto batch = list->read())
        EXPECT_EQ(batch.get(), src_batch.get());
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

TEST(FilterColumnsBlockInputStream, StreamSmoke)
{
    std::shared_ptr<arrow::RecordBatch> src_batch = TestBatch(10, "int64");
    BlockInputStreamPtr stream = std::make_shared<OneBlockInputStream>(src_batch);

    std::vector<std::string> names = {"int64"};
    auto proj = std::make_shared<FilterColumnsBlockInputStream>(stream, names, true);
    EXPECT_EQ(proj->getName(), "FilterColumns");

    auto batch = proj->read();
    EXPECT_NE(batch.get(), nullptr);
    EXPECT_EQ(batch->num_columns(), 1);

    names = {};
    proj = std::make_shared<FilterColumnsBlockInputStream>(stream, names, true);
    batch = proj->read();

    EXPECT_EQ(batch.get(), nullptr);
}

TEST(ReverseBlockInputStream, StreamSmoke)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch();
    auto one = std::make_shared<OneBlockInputStream>(batch);

    auto check = std::make_shared<ReverseBlockInputStream>(one);
    auto res_batch = check->read();
    EXPECT_NE(res_batch.get(), nullptr);
    EXPECT_EQ(res_batch->num_rows(), batch->num_rows());
    EXPECT_EQ(res_batch->num_columns(), batch->num_columns());
}

TEST(CheckSortedBlockInputStream, StreamSmoke)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch();
    auto one = std::make_shared<OneBlockInputStream>(batch);

    CHY::SortDescription sort_descr;
    sort_descr.sorting_key = batch->schema();
    sort_descr.directions = {1};

    auto check = std::make_shared<CHY::CheckSortedBlockInputStream>(one, sort_descr);
    check->read();

    sort_descr.directions = {-1};
    check = std::make_shared<CHY::CheckSortedBlockInputStream>(one, sort_descr);
    check->read();
}

TEST(UnionBlockInputStream, StreamSmoke)
{
    std::shared_ptr<arrow::RecordBatch> src_batch = TestBatch();
    BlockInputStreams streams;
    streams.reserve(128);
    for (size_t i = 0; i < 128; ++i)
        streams.push_back(std::make_shared<OneBlockInputStream>(src_batch));

    BlockInputStreamPtr additional = {};
    auto union_stream = std::make_shared<UnionBlockInputStream>(streams, additional, 16);
    EXPECT_EQ(union_stream->getName(), "Union");

    while (auto batch = union_stream->read())
        EXPECT_EQ(batch.get(), src_batch.get());
}

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
