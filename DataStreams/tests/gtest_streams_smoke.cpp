#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/FilterColumnsBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/ParallelInputsSink.h>
#include <DataStreams/ReverseBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <YdbModes/CheckSortedBlockInputStream.h>
#include <YdbModes/ExpressionBlockInputStream.h>
#include <YdbModes/SsaProgram.h>
#include <YdbModes/helpers.h>
#include <gtest/gtest.h>

using namespace AH;

static std::shared_ptr<arrow::RecordBatch> TestBatch(int num_rows = 10, std::string column_name = "int64")
{
    auto column = AHY::MakeUI64Array(42, num_rows);
    std::vector<std::shared_ptr<arrow::Field>> fields = {std::make_shared<arrow::Field>(column_name, column->type())};
    auto schema = std::make_shared<arrow::Schema>(fields);

    // Echo the totals to the client
    auto ret = arrow::RecordBatch::Make(schema, column->length(), {column});
    EXPECT_TRUE(!!ret);
    return ret;
}

TEST(StreamSmoke, NullBlockInputStream)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch();
    NullBlockInputStream stream(batch->schema());
    EXPECT_EQ(stream.getName(), "Null");
    EXPECT_EQ(stream.read().get(), nullptr);
}

TEST(StreamSmoke, OneBlockInputStream)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch();
    OneBlockInputStream stream(batch);
    EXPECT_EQ(stream.getName(), "One");
    batch = stream.read();
}

TEST(StreamSmoke, BlocksListBlockInputStream)
{
    std::shared_ptr<arrow::RecordBatch> src_batch = TestBatch();

    auto list = std::make_shared<BlocksListBlockInputStream>(AH::BlocksList{src_batch, src_batch});
    EXPECT_EQ(list->getName(), "BlocksList");

    while (auto batch = list->read())
        EXPECT_EQ(batch.get(), src_batch.get());
}

TEST(StreamSmoke, ConcatBlockInputStream)
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

TEST(StreamSmoke, FilterColumnsBlockInputStream)
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

TEST(StreamSmoke, ReverseBlockInputStream)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch();
    auto one = std::make_shared<OneBlockInputStream>(batch);

    auto check = std::make_shared<ReverseBlockInputStream>(one);
    auto res_batch = check->read();
    EXPECT_NE(res_batch.get(), nullptr);
    EXPECT_EQ(res_batch->num_rows(), batch->num_rows());
    EXPECT_EQ(res_batch->num_columns(), batch->num_columns());
}

TEST(StreamSmoke, CheckSortedBlockInputStream)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch();
    auto one = std::make_shared<OneBlockInputStream>(batch);

    AH::SortDescription sort_descr;
    for (auto & field : batch->schema()->fields())
    {
        SortColumnDescription col_descr{field->name(), 1};
        sort_descr.push_back(col_descr);
    }

    auto check = std::make_shared<AHY::CheckSortedBlockInputStream>(one, sort_descr);
    check->read();

    sort_descr[0].direction = -1;
    check = std::make_shared<AHY::CheckSortedBlockInputStream>(one, sort_descr);
    check->read();
}

TEST(StreamSmoke, ExpressionBlockInputStream)
{
    std::shared_ptr<arrow::RecordBatch> batch = TestBatch(10, "x");
    auto one = std::make_shared<OneBlockInputStream>(batch);

    auto ssa_step = std::make_shared<AHY::ProgramStep>();
    ssa_step->assignes
        = {AHY::Assign("res1", AHY::EOperation::Add, {"x", "x"}), AHY::Assign("res2", AHY::EOperation::Subtract, {"x", "x"})};
    ssa_step->projection = {"res1", "res2"};
    auto ssa = std::make_shared<AHY::Program>(std::vector<std::shared_ptr<AHY::ProgramStep>>{ssa_step});

    auto expression = std::make_shared<AHY::ExpressionBlockInputStream>(one, ssa);
    EXPECT_EQ(expression->getName(), "SSA");
    auto res = expression->read();
    EXPECT_EQ(res->num_columns(), 2);
    EXPECT_EQ(res->num_rows(), 10);
    EXPECT_EQ(res->schema()->GetFieldIndex("res1"), 0);
    EXPECT_EQ(res->schema()->GetFieldIndex("res2"), 1);
}

TEST(StreamSmoke, UnionBlockInputStream)
{
    std::shared_ptr<arrow::RecordBatch> src_batch = TestBatch();
    BlockInputStreams streams;
    streams.reserve(128);
    for (size_t i = 0; i < 128; ++i)
        streams.push_back(std::make_shared<OneBlockInputStream>(src_batch));

    auto union_stream = std::make_shared<UnionBlockInputStream>(streams, 16);
    EXPECT_EQ(union_stream->getName(), "Union");

    while (auto batch = union_stream->read())
        EXPECT_EQ(batch.get(), src_batch.get());
}

TEST(StreamSmoke, ParallelInputsSink)
{
    std::shared_ptr<arrow::RecordBatch> src_batch = TestBatch();
    auto header = src_batch->schema();

    auto create_istreams = [&](unsigned num) -> BlockInputStreams
    {
        BlockInputStreams out;
        for (unsigned i = 0; i < num; ++i)
            out.emplace_back(std::make_shared<OneBlockInputStream>(src_batch));
        return out;
    };

    auto create_ostreams = [&](unsigned num) -> BlockOutputStreams
    {
        BlockOutputStreams out;
        for (unsigned i = 0; i < num; ++i)
            out.emplace_back(std::make_shared<NullBlockOutputStream>(header));
        return out;
    };

    BlockOutputStreamPtr output = std::make_shared<NullBlockOutputStream>(header);

    ParallelInputsSink::copyNToOne(create_istreams(3), output);
    ParallelInputsSink::copyNToOne(create_istreams(3), output, 1, 1);
    ParallelInputsSink::copyNToOne(create_istreams(3), output, 2, 1);

    auto out1 = create_ostreams(5);
    ParallelInputsSink::copyNToN(create_istreams(5), out1);

    auto out2 = create_ostreams(5);
    ParallelInputsSink::copyNToN(create_istreams(5), out2, 2);
}

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
