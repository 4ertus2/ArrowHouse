#include <array>
#include <iomanip>
#include <iostream>
#include <memory>
#include <vector>
#include <gtest/gtest.h>

#include "AggregateFunctions/AggregateFunctionAvg.h"
#include "AggregateFunctions/AggregateFunctionCount.h"
#include "AggregateFunctions/AggregateFunctionMinMaxAny.h"
#include "AggregateFunctions/AggregateFunctionSum.h"
#include "Aggregator.h"
#include "DataStreams/AggregatingBlockInputStream.h"
#include "DataStreams/MergingAggregatedBlockInputStream.h"
#include "DataStreams/OneBlockInputStream.h"

namespace AH
{

void RegisterAggregates(arrow::compute::FunctionRegistry * registry = nullptr)
{
    if (!registry)
        registry = arrow::compute::GetFunctionRegistry();

    registry->AddFunction(std::make_shared<AH::WrappedCount>("ch.count")).ok();
    registry->AddFunction(std::make_shared<AH::WrappedMin>("ch.min")).ok();
    registry->AddFunction(std::make_shared<AH::WrappedMax>("ch.max")).ok();
    registry->AddFunction(std::make_shared<AH::WrappedAny>("ch.any")).ok();
    registry->AddFunction(std::make_shared<AH::WrappedSum>("ch.sum")).ok();
    registry->AddFunction(std::make_shared<AH::WrappedAvg>("ch.avg")).ok();
}

// {i16, ui32, s1, s2}
Block makeTestBlock(size_t num_rows)
{
    std::vector<std::string> strings = {"abc", "def", "abcd", "defg", "ac"};

    arrow::FieldVector fields;
    arrow::ArrayVector columns;

    {
        auto field = std::make_shared<arrow::Field>("i16", arrow::int16());
        arrow::Int16Builder col;
        col.Reserve(num_rows).ok();

        for (size_t i = 0; i < num_rows; ++i)
            col.Append(i % 9).ok();

        fields.emplace_back(std::move(field));
        columns.emplace_back(*col.Finish());
    }

    {
        auto field = std::make_shared<arrow::Field>("ui32", arrow::uint32());
        arrow::UInt32Builder col;
        col.Reserve(num_rows).ok();

        for (size_t i = 0; i < num_rows; ++i)
            col.Append(i % 7).ok();

        fields.emplace_back(std::move(field));
        columns.emplace_back(*col.Finish());
    }

    {
        auto field = std::make_shared<arrow::Field>("s1", arrow::binary());
        arrow::BinaryBuilder col;
        col.Reserve(num_rows).ok();

        for (size_t i = 0; i < num_rows; ++i)
            col.Append(strings[i % strings.size()]).ok();

        fields.emplace_back(std::move(field));
        columns.emplace_back(*col.Finish());
    }

    {
        auto field = std::make_shared<arrow::Field>("s2", arrow::binary());
        arrow::BinaryBuilder col;
        col.Reserve(num_rows).ok();

        for (size_t i = 0; i < num_rows; ++i)
            col.Append(strings[i % 3]).ok();

        fields.emplace_back(std::move(field));
        columns.emplace_back(*col.Finish());
    }

    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), num_rows, columns);
}

AggregateDescription MakeCountDescription(const std::string & column_name = "cnt")
{
    auto * registry = arrow::compute::GetFunctionRegistry();
    auto func = registry->GetFunction("ch.count");
    auto wrapped = std::static_pointer_cast<ArrowAggregateFunctionWrapper>(*func);

    DataTypes empty_list_of_types;
    return AggregateDescription{.function = wrapped->getHouseFunction(empty_list_of_types), .column_name = column_name};
}

AggregateDescription MakeMinMaxAnyDescription(const std::string & agg_name, DataTypePtr data_type, uint32_t column_id)
{
    auto * registry = arrow::compute::GetFunctionRegistry();
    auto func = registry->GetFunction(agg_name);
    auto wrapped = std::static_pointer_cast<ArrowAggregateFunctionWrapper>(*func);

    DataTypes list_of_types = {data_type};
    return AggregateDescription{
        .function = wrapped->getHouseFunction(list_of_types), .arguments = {column_id}, .column_name = "res_" + agg_name};
}

AggregateDescription MakeSumDescription(DataTypePtr data_type, uint32_t column_id, const std::string & column_name = "res_sum")
{
    auto * registry = arrow::compute::GetFunctionRegistry();
    auto func = registry->GetFunction("ch.sum");
    auto wrapped = std::static_pointer_cast<ArrowAggregateFunctionWrapper>(*func);

    DataTypes list_of_types = {data_type};
    return AggregateDescription{.function = wrapped->getHouseFunction(list_of_types), .arguments = {column_id}, .column_name = column_name};
}

AggregateDescription MakeAvgDescription(DataTypePtr data_type, uint32_t column_id, const std::string & column_name = "res_avg")
{
    auto * registry = arrow::compute::GetFunctionRegistry();
    auto func = registry->GetFunction("ch.avg");
    auto wrapped = std::static_pointer_cast<ArrowAggregateFunctionWrapper>(*func);

    DataTypes list_of_types = {data_type};
    return AggregateDescription{.function = wrapped->getHouseFunction(list_of_types), .arguments = {column_id}, .column_name = column_name};
}

InputStreamPtr
MakeAggregatingStream(const InputStreamPtr & stream, const ColumnNumbers & agg_keys, const AggregateDescriptions & aggregate_descriptions)
{
    Header src_header = IBlockInputStream::getHeader(stream);
    Aggregator::Params agg_params(false, src_header, agg_keys, aggregate_descriptions, false);
    InputStreamPtr agg_stream = std::make_shared<AggregatingBlockInputStream>(stream, agg_params, false);

    ColumnNumbers merge_keys;
    {
        Header agg_header = IBlockInputStream::getHeader(agg_stream);
        for (const auto & key : agg_keys)
            merge_keys.push_back(agg_header->GetFieldIndex(src_header->field(key)->name()));
    }

    Aggregator::Params merge_params(true, IBlockInputStream::getHeader(agg_stream), merge_keys, aggregate_descriptions, false);
    return std::make_shared<MergingAggregatedBlockInputStream>(agg_stream, merge_params, true);
}

bool TestExecute(const Block & block, const ColumnNumbers & agg_keys)
{
    try
    {
        InputStreamPtr stream = std::make_shared<OneBlockInputStream>(block);

        AggregateDescription aggregate_description = MakeCountDescription();
        Aggregator::Params params(false, IBlockInputStream::getHeader(stream), agg_keys, {aggregate_description}, false);
        Aggregator aggregator(params);

        AggregatedDataVariants aggregated_data_variants;

        {
            //Stopwatch stopwatch;
            //stopwatch.start();

            aggregator.execute(stream, aggregated_data_variants);

            //stopwatch.stop();
            //std::cout << std::fixed << std::setprecision(2)
            //    << "Elapsed " << stopwatch.elapsedSeconds() << " sec."
            //    << ", " << n / stopwatch.elapsedSeconds() << " rows/sec."
            //    << std::endl;
        }
    }
    catch (const Exception & e)
    {
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}

size_t TestAggregate(const Block & block, const ColumnNumbers & agg_keys, const AggregateDescription & description)
{
    size_t rows = 0;

    try
    {
        std::cerr << "aggregate by keys: ";
        for (auto & key : agg_keys)
            std::cerr << key << " ";
        std::cerr << std::endl;

        auto stream = MakeAggregatingStream(std::make_shared<OneBlockInputStream>(block), agg_keys, {description});

        while (Block block = stream->read())
        {
            std::cerr << "result rows: " << block->num_rows() << std::endl;
            rows += block->num_rows();
        }
    }
    catch (const Exception & e)
    {
        std::cerr << e.what() << std::endl;
        return 0;
    }

    return rows;
}

}


TEST(ExecuteCount, Aggregates)
{
    AH::RegisterAggregates();

    auto block = AH::makeTestBlock(1000);

    EXPECT_TRUE(AH::TestExecute(block, {0, 1}));
    EXPECT_TRUE(AH::TestExecute(block, {1, 0}));
    EXPECT_TRUE(AH::TestExecute(block, {0, 2}));
    EXPECT_TRUE(AH::TestExecute(block, {2, 0}));
    EXPECT_TRUE(AH::TestExecute(block, {2, 3}));
    EXPECT_TRUE(AH::TestExecute(block, {0, 1, 2, 3}));
}

TEST(AggregateCount, Aggregates)
{
    AH::RegisterAggregates();

    auto block = AH::makeTestBlock(1000);

    auto agg_count = AH::MakeCountDescription();

    EXPECT_EQ(AH::TestAggregate(block, {0, 1}, agg_count), 9u * 7u);
    EXPECT_EQ(AH::TestAggregate(block, {1, 0}, agg_count), 7u * 9u);
    EXPECT_EQ(AH::TestAggregate(block, {0, 2}, agg_count), 9u * 5u);
    EXPECT_EQ(AH::TestAggregate(block, {2, 0}, agg_count), 5u * 9u);
    EXPECT_EQ(AH::TestAggregate(block, {2, 3}, agg_count), 5u * 3u);
    EXPECT_EQ(AH::TestAggregate(block, {0, 1, 2, 3}, agg_count), 9u * 7u * 5u);
}

TEST(AggregateMin, Aggregates)
{
    AH::RegisterAggregates();

    auto block = AH::makeTestBlock(1000);

    for (int i = 0; i < block->num_columns(); ++i)
    {
        auto type = block->column(i)->type();
        auto agg_descr = AH::MakeMinMaxAnyDescription("ch.min", type, i);

        EXPECT_TRUE(agg_descr.function);
        EXPECT_EQ(agg_descr.arguments.size(), 1u);

        EXPECT_EQ(AH::TestAggregate(block, {0, 1}, agg_descr), 9u * 7u);
        EXPECT_EQ(AH::TestAggregate(block, {1, 0}, agg_descr), 7u * 9u);
        EXPECT_EQ(AH::TestAggregate(block, {0, 2}, agg_descr), 9u * 5u);
        EXPECT_EQ(AH::TestAggregate(block, {2, 0}, agg_descr), 5u * 9u);
        EXPECT_EQ(AH::TestAggregate(block, {2, 3}, agg_descr), 5u * 3u);
        EXPECT_EQ(AH::TestAggregate(block, {0, 1, 2, 3}, agg_descr), 9u * 7u * 5u);
    }
}

TEST(AggregateMax, Aggregates)
{
    AH::RegisterAggregates();

    auto block = AH::makeTestBlock(1000);

    for (int i = 0; i < block->num_columns(); ++i)
    {
        auto type = block->column(i)->type();
        auto agg_descr = AH::MakeMinMaxAnyDescription("ch.max", type, i);

        EXPECT_TRUE(agg_descr.function);
        EXPECT_EQ(agg_descr.arguments.size(), 1u);

        EXPECT_EQ(AH::TestAggregate(block, {0, 1}, agg_descr), 9u * 7u);
        EXPECT_EQ(AH::TestAggregate(block, {1, 0}, agg_descr), 7u * 9u);
        EXPECT_EQ(AH::TestAggregate(block, {0, 2}, agg_descr), 9u * 5u);
        EXPECT_EQ(AH::TestAggregate(block, {2, 0}, agg_descr), 5u * 9u);
        EXPECT_EQ(AH::TestAggregate(block, {2, 3}, agg_descr), 5u * 3u);
        EXPECT_EQ(AH::TestAggregate(block, {0, 1, 2, 3}, agg_descr), 9u * 7u * 5u);
    }
}

TEST(AggregateAny, Aggregates)
{
    AH::RegisterAggregates();

    auto block = AH::makeTestBlock(1000);

    for (int i = 0; i < block->num_columns(); ++i)
    {
        auto type = block->column(i)->type();
        auto agg_descr = AH::MakeMinMaxAnyDescription("ch.any", type, i);

        EXPECT_TRUE(agg_descr.function);
        EXPECT_EQ(agg_descr.arguments.size(), 1u);

        EXPECT_EQ(AH::TestAggregate(block, {0, 1}, agg_descr), 9u * 7u);
        EXPECT_EQ(AH::TestAggregate(block, {1, 0}, agg_descr), 7u * 9u);
        EXPECT_EQ(AH::TestAggregate(block, {0, 2}, agg_descr), 9u * 5u);
        EXPECT_EQ(AH::TestAggregate(block, {2, 0}, agg_descr), 5u * 9u);
        EXPECT_EQ(AH::TestAggregate(block, {2, 3}, agg_descr), 5u * 3u);
        EXPECT_EQ(AH::TestAggregate(block, {0, 1, 2, 3}, agg_descr), 9u * 7u * 5u);
    }
}

TEST(AggregateSum, Aggregates)
{
    AH::RegisterAggregates();

    auto block = AH::makeTestBlock(1000);

    for (int i = 0; i < 2; ++i)
    {
        auto type = block->column(i)->type();
        auto agg_descr = AH::MakeSumDescription(type, i);

        EXPECT_TRUE(agg_descr.function);
        EXPECT_EQ(agg_descr.arguments.size(), 1u);

        EXPECT_EQ(AH::TestAggregate(block, {0, 1}, agg_descr), 9u * 7u);
        EXPECT_EQ(AH::TestAggregate(block, {1, 0}, agg_descr), 7u * 9u);
        EXPECT_EQ(AH::TestAggregate(block, {0, 2}, agg_descr), 9u * 5u);
        EXPECT_EQ(AH::TestAggregate(block, {2, 0}, agg_descr), 5u * 9u);
        EXPECT_EQ(AH::TestAggregate(block, {2, 3}, agg_descr), 5u * 3u);
        EXPECT_EQ(AH::TestAggregate(block, {0, 1, 2, 3}, agg_descr), 9u * 7u * 5u);
    }
}

TEST(AggregateAvg, Aggregates)
{
    AH::RegisterAggregates();

    auto block = AH::makeTestBlock(1000);

    for (int i = 0; i < 2; ++i)
    {
        auto type = block->column(i)->type();
        auto agg_descr = AH::MakeAvgDescription(type, i);

        EXPECT_TRUE(agg_descr.function);
        EXPECT_EQ(agg_descr.arguments.size(), 1u);

        EXPECT_EQ(AH::TestAggregate(block, {0, 1}, agg_descr), 9u * 7u);
        EXPECT_EQ(AH::TestAggregate(block, {1, 0}, agg_descr), 7u * 9u);
        EXPECT_EQ(AH::TestAggregate(block, {0, 2}, agg_descr), 9u * 5u);
        EXPECT_EQ(AH::TestAggregate(block, {2, 0}, agg_descr), 5u * 9u);
        EXPECT_EQ(AH::TestAggregate(block, {2, 3}, agg_descr), 5u * 3u);
        EXPECT_EQ(AH::TestAggregate(block, {0, 1, 2, 3}, agg_descr), 9u * 7u * 5u);
    }
}

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
