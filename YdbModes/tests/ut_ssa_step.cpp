#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/AggregateFunctionMinMaxAny.h>
#include <AggregateFunctions/AggregateFunctionSum.h>
#include <YdbModes/SsaProgram.h>
#include <YdbModes/helpers.h>

#include <array>
#include <memory>
#include <vector>
#include <gtest/gtest.h>

#include "arrow/api.h"
#include "arrow/compute/exec.h"
#include "arrow/type_fwd.h"

namespace arrow::compute::internal
{

void RegisterScalarArithmetic(FunctionRegistry * registry);
void RegisterScalarRoundArithmetic(FunctionRegistry * registry);
void RegisterScalarCast(FunctionRegistry * registry);

}

namespace
{

std::shared_ptr<arrow::Array> BoolVecToArray(const std::vector<bool> & vec)
{
    std::shared_ptr<arrow::Array> out;
    arrow::BooleanBuilder builder;
    for (const auto val : vec)
        EXPECT_TRUE(builder.Append(val).ok());
    EXPECT_TRUE(builder.Finish(&out).ok());
    return out;
}

std::shared_ptr<arrow::Array>
NumVecToArray(const std::shared_ptr<arrow::DataType> & type, const std::vector<double> & vec, std::optional<double> nullValue = {})
{
    std::shared_ptr<arrow::Array> out;
    AHY::SwitchType(
        type->id(),
        [&](const auto & t)
        {
            using TWrap = std::decay_t<decltype(t)>;
            using T = typename TWrap::T;

            if constexpr (arrow::is_number_type<T>::value)
            {
                typename arrow::TypeTraits<T>::BuilderType builder;
                for (const auto val : vec)
                    if (nullValue && *nullValue == val)
                        AHY::Validate(builder.AppendNull());
                    else
                        AHY::Validate(builder.Append(static_cast<typename T::c_type>(val)));
                AHY::Validate(builder.Finish(&out));
                return true;
            }
            else if constexpr (arrow::is_timestamp_type<T>())
            {
                typename arrow::TypeTraits<T>::BuilderType builder(type, arrow::default_memory_pool());
                for (const auto val : vec)
                    AHY::Validate(builder.Append(static_cast<typename T::c_type>(val)));
                AHY::Validate(builder.Finish(&out));
                return true;
            }
            return false;
        });
    return out;
}

namespace cp = ::arrow::compute;

static void RegisterHouseAggregates(cp::FunctionRegistry * registry)
{
    registry->AddFunction(std::make_shared<AH::WrappedAny>(AHY::getHouseFunctionName(AHY::EAggregate::Some))).ok();
    registry->AddFunction(std::make_shared<AH::WrappedCount>(AHY::getHouseFunctionName(AHY::EAggregate::Count))).ok();
    registry->AddFunction(std::make_shared<AH::WrappedMin>(AHY::getHouseFunctionName(AHY::EAggregate::Min))).ok();
    registry->AddFunction(std::make_shared<AH::WrappedMax>(AHY::getHouseFunctionName(AHY::EAggregate::Max))).ok();
    registry->AddFunction(std::make_shared<AH::WrappedSum>(AHY::getHouseFunctionName(AHY::EAggregate::Sum))).ok();
    //registry->AddFunction(std::make_shared<AH::WrappedAvg>(AHY::getHouseFunctionName(AHY::EAggregate::Avg))).ok();

    registry->AddFunction(std::make_shared<AH::ArrowGroupBy>(AHY::getHouseGroupByName())).ok();
}

static std::unique_ptr<cp::FunctionRegistry> CreateCustomRegistry()
{
    auto registry = cp::FunctionRegistry::Make();
    //RegisterRound(registry.get());
    //RegisterArithmetic(registry.get());
    RegisterHouseAggregates(registry.get());

    cp::internal::RegisterScalarArithmetic(registry.get());
    cp::internal::RegisterScalarRoundArithmetic(registry.get());
    cp::internal::RegisterScalarCast(registry.get());
    return registry;
}

// Creates singleton custom registry
cp::FunctionRegistry * GetCustomFunctionRegistry()
{
    static auto g_registry = CreateCustomRegistry();
    return g_registry.get();
}

// We want to have ExecContext per thread. All these context use one custom registry.
cp::ExecContext * GetCustomExecContext()
{
    static thread_local cp::ExecContext context(arrow::default_memory_pool(), nullptr, GetCustomFunctionRegistry());
    return &context;
}

}


namespace AHY
{

size_t FilterTest(std::vector<std::shared_ptr<arrow::Array>> args, EOperation op1, EOperation op2)
{
    auto schema = std::make_shared<arrow::Schema>(std::vector{
        std::make_shared<arrow::Field>("x", args.at(0)->type()),
        std::make_shared<arrow::Field>("y", args.at(1)->type()),
        std::make_shared<arrow::Field>("z", args.at(2)->type())});
    auto batch = arrow::RecordBatch::Make(schema, 3, std::vector{args.at(0), args.at(1), args.at(2)});
    EXPECT_TRUE(batch->ValidateFull().ok());

    auto step = std::make_shared<ProgramStep>();
    step->assignes = {Assign("res1", op1, {"x", "y"}), Assign("res2", op2, {"res1", "z"})};
    step->filters = {"res2"};
    step->projection = {"res1", "res2"};
    EXPECT_TRUE(applyProgram(batch, Program({step}), GetCustomExecContext()).ok());
    EXPECT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_columns(), 2);
    return batch->num_rows();
}

size_t FilterTestUnary(std::vector<std::shared_ptr<arrow::Array>> args, EOperation op1, EOperation op2)
{
    auto schema = std::make_shared<arrow::Schema>(
        std::vector{std::make_shared<arrow::Field>("x", args.at(0)->type()), std::make_shared<arrow::Field>("z", args.at(1)->type())});
    auto batch = arrow::RecordBatch::Make(schema, 3, std::vector{args.at(0), args.at(1)});
    EXPECT_TRUE(batch->ValidateFull().ok());

    auto step = std::make_shared<ProgramStep>();
    step->assignes = {Assign("res1", op1, {"x"}), Assign("res2", op2, {"res1", "z"})};
    step->filters = {"res2"};
    step->projection = {"res1", "res2"};
    auto status = applyProgram(batch, Program({step}), GetCustomExecContext());
    if (!status.ok())
        std::cerr << status.ToString() << std::endl;
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_columns(), 2);
    return batch->num_rows();
}

std::vector<bool> LikeTest(const std::vector<std::string> & data, EOperation op, const std::string & pattern, bool ignoreCase = false)
{
    auto schema = std::make_shared<arrow::Schema>(std::vector{std::make_shared<arrow::Field>("x", arrow::utf8())});
    arrow::StringBuilder sb;
    sb.AppendValues(data).ok();
    auto batch = arrow::RecordBatch::Make(schema, data.size(), {*sb.Finish()});
    EXPECT_TRUE(batch->ValidateFull().ok());

    auto step = std::make_shared<ProgramStep>();
    step->assignes = {Assign("res", op, {"x"}, std::make_shared<arrow::compute::MatchSubstringOptions>(pattern, ignoreCase))};
    step->projection = {"res"};
    auto status = applyProgram(batch, Program({step}), GetCustomExecContext());
    if (!status.ok())
        std::cerr << status.ToString() << std::endl;
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_columns(), 1);

    auto & resColumn = static_cast<const arrow::BooleanArray &>(*batch->GetColumnByName("res"));
    std::vector<bool> vec;
    for (int i = 0; i < resColumn.length(); ++i)
    {
        EXPECT_TRUE(!resColumn.IsNull(i)); // TODO
        vec.push_back(resColumn.Value(i));
    }
    return vec;
}

enum class ETest
{
    DEFAULT,
    EMPTY,
    ONE_VALUE
};

struct SumData
{
    static std::shared_ptr<arrow::RecordBatch> Data(ETest test, std::shared_ptr<arrow::Schema> & schema, bool nullable)
    {
        std::optional<double> null;
        if (nullable)
            null = 0;

        if (test == ETest::DEFAULT)
        {
            return arrow::RecordBatch::Make(
                schema,
                4,
                std::vector{NumVecToArray(arrow::int16(), {-1, 0, 0, -1}, null), NumVecToArray(arrow::uint32(), {1, 0, 0, 1}, null)});
        }
        else if (test == ETest::EMPTY)
        {
            return arrow::RecordBatch::Make(schema, 0, std::vector{NumVecToArray(arrow::int16(), {}), NumVecToArray(arrow::uint32(), {})});
        }
        else if (test == ETest::ONE_VALUE)
        {
            return arrow::RecordBatch::Make(
                schema, 1, std::vector{NumVecToArray(arrow::int16(), {1}), NumVecToArray(arrow::uint32(), {0}, null)});
        }
        return {};
    }

    static void CheckResult(ETest test, const std::shared_ptr<arrow::RecordBatch> & batch, uint32_t numKeys, bool nullable)
    {
        EXPECT_EQ(batch->num_columns(), numKeys + 2);
        EXPECT_EQ(batch->column(0)->type_id(), arrow::Type::INT64);
        EXPECT_EQ(batch->column(1)->type_id(), arrow::Type::UINT64);
        EXPECT_EQ(batch->column(2)->type_id(), arrow::Type::INT16);
        if (numKeys == 2)
            EXPECT_EQ(batch->column(3)->type_id(), arrow::Type::UINT32);

        if (test == ETest::EMPTY)
        {
            EXPECT_EQ(batch->num_rows(), 0);
            return;
        }

        auto & aggX = static_cast<arrow::Int64Array &>(*batch->column(0));
        auto & aggY = static_cast<arrow::UInt64Array &>(*batch->column(1));
        auto & colX = static_cast<arrow::Int16Array &>(*batch->column(2));

        if (test == ETest::ONE_VALUE)
        {
            EXPECT_EQ(batch->num_rows(), 1);

            EXPECT_EQ(aggX.Value(0), 1);
            if (nullable)
            {
                EXPECT_TRUE(aggY.IsNull(0));
            }
            else
            {
                EXPECT_TRUE(!aggY.IsNull(0));
                EXPECT_EQ(aggY.Value(0), 0);
            }
            return;
        }

        EXPECT_EQ(batch->num_rows(), 2);

        for (uint32_t row = 0; row < 2; ++row)
        {
            if (colX.IsNull(row))
            {
                EXPECT_TRUE(aggX.IsNull(row));
                EXPECT_TRUE(aggY.IsNull(row));
            }
            else
            {
                EXPECT_TRUE(!aggX.IsNull(row));
                EXPECT_TRUE(!aggY.IsNull(row));
                if (colX.Value(row) == 0)
                {
                    EXPECT_EQ(aggX.Value(row), 0);
                    EXPECT_EQ(aggY.Value(row), 0);
                }
                else if (colX.Value(row) == -1)
                {
                    EXPECT_EQ(aggX.Value(row), -2);
                    EXPECT_EQ(aggY.Value(row), 2);
                }
                else
                {
                    EXPECT_TRUE(false);
                }
            }
        }
    }
};

struct MinMaxSomeData
{
    static std::shared_ptr<arrow::RecordBatch> Data(ETest /*test*/, std::shared_ptr<arrow::Schema> & schema, bool nullable)
    {
        std::optional<double> null;
        if (nullable)
            null = 0;

        return arrow::RecordBatch::Make(
            schema, 1, std::vector{NumVecToArray(arrow::int16(), {1}), NumVecToArray(arrow::uint32(), {0}, null)});
    }

    static void CheckResult(ETest /*test*/, const std::shared_ptr<arrow::RecordBatch> & batch, uint32_t numKeys, bool nullable)
    {
        EXPECT_EQ(numKeys, 1);

        EXPECT_EQ(batch->num_columns(), numKeys + 2);
        EXPECT_EQ(batch->column(0)->type_id(), arrow::Type::INT16);
        EXPECT_EQ(batch->column(1)->type_id(), arrow::Type::UINT32);
        EXPECT_EQ(batch->column(2)->type_id(), arrow::Type::INT16);

        auto & aggX = static_cast<arrow::Int16Array &>(*batch->column(0));
        auto & aggY = static_cast<arrow::UInt32Array &>(*batch->column(1));
        auto & colX = static_cast<arrow::Int16Array &>(*batch->column(2));

        EXPECT_EQ(batch->num_rows(), 1);

        EXPECT_EQ(colX.Value(0), 1);
        EXPECT_EQ(aggX.Value(0), 1);
        if (nullable)
        {
            EXPECT_TRUE(aggY.IsNull(0));
        }
        else
        {
            EXPECT_TRUE(!aggY.IsNull(0));
            EXPECT_EQ(aggY.Value(0), 0);
        }
        return;
    }
};

void GroupByXY(bool nullable, uint32_t numKeys, ETest test = ETest::DEFAULT, EAggregate aggFunc = EAggregate::Sum)
{
    auto schema = std::make_shared<arrow::Schema>(
        std::vector{std::make_shared<arrow::Field>("x", arrow::int16()), std::make_shared<arrow::Field>("y", arrow::uint32())});

    std::shared_ptr<arrow::RecordBatch> batch;
    switch (aggFunc)
    {
        case EAggregate::Sum:
            batch = SumData::Data(test, schema, nullable);
            break;
        case EAggregate::Min:
        case EAggregate::Max:
        case EAggregate::Some:
            batch = MinMaxSomeData::Data(test, schema, nullable);
            break;
        default:
            break;
    }
    EXPECT_TRUE(batch);
    auto status = batch->ValidateFull();
    if (!status.ok())
        std::cerr << status.ToString() << std::endl;
    EXPECT_TRUE(status.ok());

    auto step = std::make_shared<ProgramStep>();
    step->groupBy = {AggregateAssign("agg_x", aggFunc, {"x"}), AggregateAssign("agg_y", aggFunc, {"y"})};
    step->groupByKeys.push_back("x");
    if (numKeys == 2)
        step->groupByKeys.push_back("y");

    status = applyProgram(batch, Program({step}), GetCustomExecContext());
    if (!status.ok())
        std::cerr << status.ToString() << std::endl;
    EXPECT_TRUE(status.ok());

    status = batch->ValidateFull();
    if (!status.ok())
        std::cerr << status.ToString() << std::endl;
    EXPECT_TRUE(status.ok());

    switch (aggFunc)
    {
        case EAggregate::Sum:
            SumData::CheckResult(test, batch, numKeys, nullable);
            break;
        case EAggregate::Min:
        case EAggregate::Max:
        case EAggregate::Some:
            MinMaxSomeData::CheckResult(test, batch, numKeys, nullable);
            break;
        default:
            break;
    }
}

}

using namespace AHY;

#if 0
TEST(Round0, SSA)
{
    for (auto eop : {EOperation::Round, EOperation::RoundBankers, EOperation::RoundToExp2})
    {
        auto x = NumVecToArray(arrow::float64(), {32.3, 12.5, 34.7});
        auto z = arrow::compute::CallFunction(getFunctionName(eop), {x}, GetCustomExecContext());
        EXPECT_EQ(FilterTestUnary({x, z->make_array()}, eop, EOperation::Equal), 3);
    }
}
#else
TEST(Round0, SSA)
{
    for (auto eop : {EOperation::Round})
    {
        auto x = NumVecToArray(arrow::float64(), {32.3, 12.5, 34.7});
        auto z = arrow::compute::CallFunction(getFunctionName(eop), {x}, GetCustomExecContext());
        auto res = FilterTestUnary({x, z->make_array()}, eop, EOperation::Equal);
        std::cerr << res << std::endl;
        EXPECT_EQ(res, 3);
    }
}
#endif

TEST(Round1, SSA)
{
    for (auto eop : {EOperation::Ceil, EOperation::Floor, EOperation::Trunc})
    {
        auto x = NumVecToArray(arrow::float64(), {32.3, 12.5, 34.7});
        auto z = arrow::compute::CallFunction(getFunctionName(eop), {x});
        EXPECT_EQ(FilterTestUnary({x, z->make_array()}, eop, EOperation::Equal), 3);
    }
}

TEST(Filter, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
    auto y = NumVecToArray(arrow::uint32(), {10, 34, 8});
    auto z = NumVecToArray(arrow::int64(), {33, 70, 12});
    EXPECT_EQ(FilterTest({x, y, z}, EOperation::Add, EOperation::Less), 2);
}

TEST(Add, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
    auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
    auto z = arrow::compute::CallFunction("add", {x, y});
    EXPECT_EQ(FilterTest({x, y, z->make_array()}, EOperation::Add, EOperation::Equal), 3);
}

TEST(Substract, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
    auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
    auto z = arrow::compute::CallFunction("subtract", {x, y});
    EXPECT_EQ(FilterTest({x, y, z->make_array()}, EOperation::Subtract, EOperation::Equal), 3);
}

TEST(Multiply, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
    auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
    auto z = arrow::compute::CallFunction("multiply", {x, y});
    EXPECT_EQ(FilterTest({x, y, z->make_array()}, EOperation::Multiply, EOperation::Equal), 3);
}

TEST(Divide, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
    auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
    auto z = arrow::compute::CallFunction("divide", {x, y});
    EXPECT_EQ(FilterTest({x, y, z->make_array()}, EOperation::Divide, EOperation::Equal), 3);
}

#if 0
TEST(Gcd, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
    auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
    auto z = arrow::compute::CallFunction("gcd", {x, y}, GetCustomExecContext());
    EXPECT_EQ(FilterTest({x, y, z->make_array()}, EOperation::Gcd, EOperation::Equal), 3);
}

TEST(Lcm, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
    auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
    auto z = arrow::compute::CallFunction("lcm", {x, y}, GetCustomExecContext());
    EXPECT_EQ(FilterTest({x, y, z->make_array()}, EOperation::Lcm, EOperation::Equal), 3);
}

TEST(Modulo, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
    auto y = NumVecToArray(arrow::int32(), {3, 5, 2});
    auto z = arrow::compute::CallFunction("modulo", {x, y}, GetCustomExecContext());
    EXPECT_EQ(FilterTest({x, y, z->make_array()}, EOperation::Modulo, EOperation::Equal), 3);
}

TEST(ModOrZero, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
    auto y = NumVecToArray(arrow::int32(), {3, 5, 0});
    auto z = arrow::compute::CallFunction("modOrZero", {x, y}, GetCustomExecContext());
    EXPECT_EQ(FilterTest({x, y, z->make_array()}, EOperation::ModuloOrZero, EOperation::Equal), 3);
}
#endif
TEST(Abs, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {-64, -16, 8});
    auto z = arrow::compute::CallFunction("abs", {x});
    EXPECT_EQ(FilterTestUnary({x, z->make_array()}, EOperation::Abs, EOperation::Equal), 3);
}

TEST(Negate, SSA)
{
    auto x = NumVecToArray(arrow::int32(), {-64, -16, 8});
    auto z = arrow::compute::CallFunction("negate", {x});
    EXPECT_EQ(FilterTestUnary({x, z->make_array()}, EOperation::Negate, EOperation::Equal), 3);
}

TEST(Compares, SSA)
{
    for (auto eop :
         {EOperation::Equal, EOperation::Less, EOperation::Greater, EOperation::GreaterEqual, EOperation::LessEqual, EOperation::NotEqual})
    {
        auto x = NumVecToArray(arrow::int32(), {64, 5, 1});
        auto y = NumVecToArray(arrow::int32(), {64, 1, 5});
        auto z = arrow::compute::CallFunction(getFunctionName(eop), {x, y});
        EXPECT_EQ(FilterTest({x, y, z->make_array()}, eop, EOperation::Equal), 3);
    }
}

TEST(Logic0, SSA)
{
    for (auto eop : {EOperation::And, EOperation::Or, EOperation::Xor})
    {
        auto x = BoolVecToArray({true, false, false});
        auto y = BoolVecToArray({true, true, false});
        auto z = arrow::compute::CallFunction(getFunctionName(eop), {x, y});
        EXPECT_EQ(FilterTest({x, y, z->make_array()}, eop, EOperation::Equal), 3);
    }
}

TEST(Logic1, SSA)
{
    auto x = BoolVecToArray({true, false, false});
    auto z = arrow::compute::CallFunction("invert", {x});
    EXPECT_EQ(FilterTestUnary({x, z->make_array()}, EOperation::Invert, EOperation::Equal), 3);
}

TEST(StartsWith, SSA)
{
    std::vector<bool> res = LikeTest({"aa", "abaaba", "baa", ""}, EOperation::StartsWith, "aa");
    EXPECT_EQ(res.size(), 4);
    EXPECT_EQ(res[0], true);
    EXPECT_EQ(res[1], false);
    EXPECT_EQ(res[2], false);
    EXPECT_EQ(res[3], false);
}

TEST(EndsWith, SSA)
{
    std::vector<bool> res = LikeTest({"aa", "abaaba", "baa", ""}, EOperation::EndsWith, "aa");
    EXPECT_EQ(res.size(), 4);
    EXPECT_EQ(res[0], true);
    EXPECT_EQ(res[1], false);
    EXPECT_EQ(res[2], true);
    EXPECT_EQ(res[3], false);
}

TEST(MatchSubstring, SSA)
{
    std::vector<bool> res = LikeTest({"aa", "abaaba", "baa", ""}, EOperation::MatchSubstring, "aa");
    EXPECT_EQ(res.size(), 4);
    EXPECT_EQ(res[0], true);
    EXPECT_EQ(res[1], true);
    EXPECT_EQ(res[2], true);
    EXPECT_EQ(res[3], false);
}

TEST(StartsWithIgnoreCase, SSA)
{
    std::vector<bool> res = LikeTest({"Aa", "abAaba", "baA", ""}, EOperation::StartsWith, "aA", true);
    EXPECT_EQ(res.size(), 4);
    EXPECT_EQ(res[0], true);
    EXPECT_EQ(res[1], false);
    EXPECT_EQ(res[2], false);
    EXPECT_EQ(res[3], false);
}

TEST(EndsWithIgnoreCase, SSA)
{
    std::vector<bool> res = LikeTest({"Aa", "abAaba", "baA", ""}, EOperation::EndsWith, "aA", true);
    EXPECT_EQ(res.size(), 4);
    EXPECT_EQ(res[0], true);
    EXPECT_EQ(res[1], false);
    EXPECT_EQ(res[2], true);
    EXPECT_EQ(res[3], false);
}

TEST(MatchSubstringIgnoreCase, SSA)
{
    std::vector<bool> res = LikeTest({"Aa", "abAaba", "baA", ""}, EOperation::MatchSubstring, "aA", true);
    EXPECT_EQ(res.size(), 4);
    EXPECT_EQ(res[0], true);
    EXPECT_EQ(res[1], true);
    EXPECT_EQ(res[2], true);
    EXPECT_EQ(res[3], false);
}

TEST(ScalarTest, SSA)
{
    auto schema = std::make_shared<arrow::Schema>(
        std::vector{std::make_shared<arrow::Field>("x", arrow::int64()), std::make_shared<arrow::Field>("filter", arrow::boolean())});
    auto batch = arrow::RecordBatch::Make(
        schema, 4, std::vector{NumVecToArray(arrow::int64(), {64, 5, 1, 43}), BoolVecToArray({true, false, false, true})});
    EXPECT_TRUE(batch->ValidateFull().ok());

    auto step = std::make_shared<ProgramStep>();
    step->assignes = {Assign("y", 56), Assign("res", EOperation::Add, {"x", "y"})};
    step->filters = {"filter"};
    step->projection = {"res", "filter"};
    EXPECT_TRUE(applyProgram(batch, Program({step}), GetCustomExecContext()).ok());
    EXPECT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_columns(), 2);
    EXPECT_EQ(batch->num_rows(), 2);
}

TEST(Projection, SSA)
{
    auto schema = std::make_shared<arrow::Schema>(
        std::vector{std::make_shared<arrow::Field>("x", arrow::int64()), std::make_shared<arrow::Field>("y", arrow::boolean())});
    auto batch = arrow::RecordBatch::Make(
        schema, 4, std::vector{NumVecToArray(arrow::int64(), {64, 5, 1, 43}), BoolVecToArray({true, false, false, true})});
    EXPECT_TRUE(batch->ValidateFull().ok());

    auto step = std::make_shared<ProgramStep>();
    step->projection = {"x"};
    EXPECT_TRUE(applyProgram(batch, Program({step}), GetCustomExecContext()).ok());
    EXPECT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_columns(), 1);
    EXPECT_EQ(batch->num_rows(), 4);
}

TEST(MinMax, SSA)
{
    auto tsType = arrow::timestamp(arrow::TimeUnit::MICRO);


    auto schema = std::make_shared<arrow::Schema>(
        std::vector{std::make_shared<arrow::Field>("x", arrow::int16()), std::make_shared<arrow::Field>("y", tsType)});
    auto batch = arrow::RecordBatch::Make(
        schema, 4, std::vector{NumVecToArray(arrow::int16(), {1, 0, -1, 2}), NumVecToArray(tsType, {1, 4, 2, 3})});
    EXPECT_TRUE(batch->ValidateFull().ok());

    auto step = std::make_shared<ProgramStep>();
    step->groupBy = {AggregateAssign("min_x", EAggregate::Min, {"x"}), AggregateAssign("max_y", EAggregate::Max, {"y"})};
    EXPECT_TRUE(applyProgram(batch, Program({step}), GetCustomExecContext()).ok());
    EXPECT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_columns(), 2);
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->column(0)->type_id(), arrow::Type::INT16);
    EXPECT_EQ(batch->column(1)->type_id(), arrow::Type::TIMESTAMP);

    EXPECT_EQ(static_cast<arrow::Int16Array &>(*batch->column(0)).Value(0), -1);
    EXPECT_EQ(static_cast<arrow::TimestampArray &>(*batch->column(1)).Value(0), 4);
}

TEST(Sum, SSA)
{
    auto schema = std::make_shared<arrow::Schema>(
        std::vector{std::make_shared<arrow::Field>("x", arrow::int16()), std::make_shared<arrow::Field>("y", arrow::uint32())});
    auto batch = arrow::RecordBatch::Make(
        schema, 4, std::vector{NumVecToArray(arrow::int16(), {-1, 0, 1, 2}), NumVecToArray(arrow::uint32(), {1, 2, 3, 4})});
    EXPECT_TRUE(batch->ValidateFull().ok());

    auto step = std::make_shared<ProgramStep>();
    step->groupBy = {AggregateAssign("sum_x", EAggregate::Sum, {"x"}), AggregateAssign("sum_y", EAggregate::Sum, {"y"})};
    EXPECT_TRUE(applyProgram(batch, Program({step}), GetCustomExecContext()).ok());
    EXPECT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_columns(), 2);
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->column(0)->type_id(), arrow::Type::INT64);
    EXPECT_EQ(batch->column(1)->type_id(), arrow::Type::UINT64);

    EXPECT_EQ(static_cast<arrow::Int64Array &>(*batch->column(0)).Value(0), 2);
    EXPECT_EQ(static_cast<arrow::UInt64Array &>(*batch->column(1)).Value(0), 10);
}

TEST(SumGroupBy, SSA)
{
    GroupByXY(true, 1);
    GroupByXY(true, 2);

    GroupByXY(true, 1, ETest::EMPTY);
    GroupByXY(true, 2, ETest::EMPTY);

    GroupByXY(true, 1, ETest::ONE_VALUE);
}

TEST(SumGroupByNotNull, SSA)
{
    GroupByXY(false, 1);
    GroupByXY(false, 2);

    GroupByXY(false, 1, ETest::EMPTY);
    GroupByXY(false, 2, ETest::EMPTY);

    GroupByXY(false, 1, ETest::ONE_VALUE);
}

TEST(MinMaxSomeGroupBy, SSA)
{
    GroupByXY(true, 1, ETest::ONE_VALUE, EAggregate::Min);
    GroupByXY(true, 1, ETest::ONE_VALUE, EAggregate::Max);
    GroupByXY(true, 1, ETest::ONE_VALUE, EAggregate::Some);
}

TEST(MinMaxSomeGroupByNotNull, SSA)
{
    GroupByXY(false, 1, ETest::ONE_VALUE, EAggregate::Min);
    GroupByXY(false, 1, ETest::ONE_VALUE, EAggregate::Max);
    GroupByXY(false, 1, ETest::ONE_VALUE, EAggregate::Some);
}

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}