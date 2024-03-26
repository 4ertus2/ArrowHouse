#pragma once

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/compute/api_aggregate.h"

namespace AHY
{

enum class EOperation
{
    Unspecified = 0,
    Constant,
    //
    CastBoolean,
    CastInt8,
    CastInt16,
    CastInt32,
    CastInt64,
    CastUInt8,
    CastUInt16,
    CastUInt32,
    CastUInt64,
    CastFloat,
    CastDouble,
    CastBinary,
    CastFixedSizeBinary,
    CastString,
    CastTimestamp,
    //
    IsValid,
    IsNull,
    //
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    //
    Invert,
    And,
    Or,
    Xor,
    //
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Abs,
    Negate,
#if 0
    Gcd,
    Lcm,
    ModuloOrZero,
#endif
    AddNotNull,
    SubtractNotNull,
    MultiplyNotNull,
    DivideNotNull,
    //
    BinaryLength,
    MatchSubstring,
    MatchLike,
    StartsWith,
    EndsWith,
#if 0
    // math
    Acosh,
    Atanh,
    Cbrt,
    Cosh,
    E,
    Erf,
    Erfc,
    Exp,
    Exp2,
    Exp10,
    Hypot,
    Lgamma,
    Pi,
    Sinh,
    Sqrt,
    Tgamma,
#endif
    // round
    Floor,
    Ceil,
    Trunc,
    Round,
#if 0
    RoundBankers,
    RoundToExp2,
#endif
};

enum class EAggregate
{
    Unspecified = 0,
    Some = 1,
    Count = 2,
    Min = 3,
    Max = 4,
    Sum = 5,
    //Avg = 6,
};

const char * getFunctionName(EOperation op);
const char * getFunctionName(EAggregate op);
const char * getHouseFunctionName(EAggregate op);
inline const char * getHouseGroupByName()
{
    return "ch.group_by";
}
EOperation validateOperation(EOperation op, uint32_t argsSize);

class Assign
{
public:
    Assign(const std::string & name_, EOperation op, std::vector<std::string> && args)
        : name(name_), operation(validateOperation(op, args.size())), arguments(std::move(args)), funcOpts(nullptr)
    {
    }

    Assign(
        const std::string & name_,
        EOperation op,
        std::vector<std::string> && args,
        std::shared_ptr<arrow::compute::FunctionOptions> funcOpts)
        : name(name_), operation(validateOperation(op, args.size())), arguments(std::move(args)), funcOpts(funcOpts)
    {
    }

    explicit Assign(const std::string & name_, bool value)
        : name(name_), operation(EOperation::Constant), constant(std::make_shared<arrow::BooleanScalar>(value)), funcOpts(nullptr)
    {
    }

    explicit Assign(const std::string & name_, int32_t value)
        : name(name_), operation(EOperation::Constant), constant(std::make_shared<arrow::Int32Scalar>(value)), funcOpts(nullptr)
    {
    }

    explicit Assign(const std::string & name_, uint32_t value)
        : name(name_), operation(EOperation::Constant), constant(std::make_shared<arrow::UInt32Scalar>(value)), funcOpts(nullptr)
    {
    }

    explicit Assign(const std::string & name_, int64_t value)
        : name(name_), operation(EOperation::Constant), constant(std::make_shared<arrow::Int64Scalar>(value)), funcOpts(nullptr)
    {
    }

    explicit Assign(const std::string & name_, uint64_t value)
        : name(name_), operation(EOperation::Constant), constant(std::make_shared<arrow::UInt64Scalar>(value)), funcOpts(nullptr)
    {
    }

    explicit Assign(const std::string & name_, float value)
        : name(name_), operation(EOperation::Constant), constant(std::make_shared<arrow::FloatScalar>(value)), funcOpts(nullptr)
    {
    }

    explicit Assign(const std::string & name_, double value)
        : name(name_), operation(EOperation::Constant), constant(std::make_shared<arrow::DoubleScalar>(value)), funcOpts(nullptr)
    {
    }

    explicit Assign(const std::string & name_, const std::string & value)
        : name(name_), operation(EOperation::Constant), constant(std::make_shared<arrow::StringScalar>(value)), funcOpts(nullptr)
    {
    }

    Assign(const std::string & name_, const std::shared_ptr<arrow::Scalar> & value)
        : name(name_), operation(EOperation::Constant), constant(value), funcOpts(nullptr)
    {
    }

    bool IsConstant() const { return operation == EOperation::Constant; }
    bool IsOk() const { return operation != EOperation::Unspecified; }
    EOperation GetOperation() const { return operation; }
    const std::vector<std::string> & GetArguments() const { return arguments; }
    std::shared_ptr<arrow::Scalar> GetConstant() const { return constant; }
    const std::string & GetName() const { return name; }
    const arrow::compute::FunctionOptions * GetFunctionOptions() const { return funcOpts.get(); }

private:
    std::string name;
    EOperation operation{EOperation::Unspecified};
    std::vector<std::string> arguments;
    std::shared_ptr<arrow::Scalar> constant;
    std::shared_ptr<arrow::compute::FunctionOptions> funcOpts;
};

class AggregateAssign
{
public:
    AggregateAssign(const std::string & name_, EAggregate op = EAggregate::Unspecified) : name(name_), operation(op)
    {
        if (op != EAggregate::Count)
            op = EAggregate::Unspecified;
    }

    AggregateAssign(const std::string & name, EAggregate op, std::string && arg) : name(name), operation(op), arguments({std::move(arg)})
    {
        if (arguments.empty())
            op = EAggregate::Unspecified;
    }

    bool IsOk() const { return operation != EAggregate::Unspecified; }
    EAggregate GetOperation() const { return operation; }
    const std::vector<std::string> & GetArguments() const { return arguments; }
    std::vector<std::string> & MutableArguments() { return arguments; }
    const std::string & GetName() const { return name; }
    const arrow::compute::ScalarAggregateOptions & GetAggregateOptions() const { return scalarOpts; }

private:
    std::string name;
    EAggregate operation{EAggregate::Unspecified};
    std::vector<std::string> arguments;
    arrow::compute::ScalarAggregateOptions scalarOpts; // TODO: make correct options
};

/// Group of commands that finishes with projection. Steps add locality for columns definition.
///
/// In step we have non-decreasing count of columns (line to line) till projection. So columns are either source
/// for the step either defined in this step.
/// It's also possible to use several filters in step. They would be applyed after assignes, just before projection.
/// "Filter (a > 0 AND b <= 42)" is logically equal to "Filret a > 0; Filter b <= 42"
/// Step combines (f1 AND f2 AND ... AND fn) into one filter and applies it once. You have to split filters in different
/// steps if you want to run them separately. I.e. if you expect that f1 is fast and leads to a small row-set.
/// Then when we place all assignes before filters they have the same row count. It's possible to run them in parallel.
struct ProgramStep
{
    std::vector<Assign> assignes;
    std::vector<std::string> filters; // List of filter columns. Implicit "Filter by (f1 AND f2 AND .. AND fn)"
    std::vector<AggregateAssign> groupBy;
    std::vector<std::string> groupByKeys; // TODO: it's possible to use them without GROUP BY for DISTINCT
    std::vector<std::string> projection; // Step's result columns (remove others)

    struct DatumBatch
    {
        std::shared_ptr<arrow::Schema> Schema;
        std::vector<arrow::Datum> Datums;
        int64_t Rows{};

        arrow::Status AddColumn(const std::string & name, arrow::Datum && column);
        arrow::Result<arrow::Datum> GetColumnByName(const std::string & name) const;
        std::shared_ptr<arrow::RecordBatch> ToRecordBatch() const;
        static std::shared_ptr<ProgramStep::DatumBatch> FromRecordBatch(std::shared_ptr<arrow::RecordBatch> & batch);
    };

    bool empty() const { return assignes.empty() && filters.empty() && projection.empty() && groupBy.empty() && groupByKeys.empty(); }

    arrow::Status apply(std::shared_ptr<arrow::RecordBatch> & batch, arrow::compute::ExecContext * ctx) const;

    arrow::Status applyAssignes(DatumBatch & batch, arrow::compute::ExecContext * ctx) const;
    arrow::Status applyAggregates(DatumBatch & batch, arrow::compute::ExecContext * ctx) const;
    arrow::Status applyFilters(DatumBatch & batch) const;
    arrow::Status applyProjection(std::shared_ptr<arrow::RecordBatch> & batch) const;
    arrow::Status applyProjection(DatumBatch & batch) const;
};

struct Program
{
    std::vector<std::shared_ptr<ProgramStep>> steps;

    Program() = default;
    Program(std::vector<std::shared_ptr<ProgramStep>> && steps_) : steps(std::move(steps_)) { }

    arrow::Status applyTo(std::shared_ptr<arrow::RecordBatch> & batch, arrow::compute::ExecContext * ctx) const
    {
        try
        {
            for (auto & step : steps)
            {
                auto status = step->apply(batch, ctx);
                if (!status.ok())
                    return status;
            }
        }
        catch (const std::exception & ex)
        {
            return arrow::Status::Invalid(ex.what());
        }
        return arrow::Status::OK();
    }
};

inline arrow::Status
applyProgram(std::shared_ptr<arrow::RecordBatch> & batch, const Program & program, arrow::compute::ExecContext * ctx = nullptr)
{
    return program.applyTo(batch, ctx);
}

}
