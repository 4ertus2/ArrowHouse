// The code in this file is based on original YDB source code
// which is licensed under Apache license v2.0
// See: https://github.com/ydb-platform/ydb

#pragma once
#include <compare>
#include <arrow/api.h>
#include "helpers.h"

namespace CH
{

using ArrayVec = std::vector<std::shared_ptr<arrow::Array>>;

template <typename ArrayVecPtr>
class ReplaceKeyTemplate
{
public:
    static constexpr bool is_owning = std::is_same_v<ArrayVecPtr, std::shared_ptr<ArrayVec>>;
#if 0
    void ShrinkToFit()
    {
        if (columns->front()->length() != 1)
        {
            auto columnsNew = std::make_shared<ArrayVec>();
            for (auto && i : *columns)
                columnsNew->emplace_back(NArrow::CopyRecords(i, {position}));
            columns = columnsNew;
            position = 0;
        }
    }
#endif
    ReplaceKeyTemplate(ArrayVecPtr columns, const uint64_t position) : columns(columns), position(position) { }

    template <typename T = ArrayVecPtr>
    requires is_owning
    ReplaceKeyTemplate(ArrayVec && columns, const uint64_t position)
        : columns(std::make_shared<ArrayVec>(std::move(columns))), position(position)
    {
    }

    template <typename T>
    bool operator==(const ReplaceKeyTemplate<T> & key) const
    {
        for (int i = 0; i < Size(); ++i)
        {
            auto cmp = CompareColumnValue(i, key, i);
            if (std::is_neq(cmp))
                return false;
        }
        return true;
    }

    template <typename T>
    std::partial_ordering operator<=>(const ReplaceKeyTemplate<T> & key) const
    {
        for (int i = 0; i < Size(); ++i)
        {
            auto cmp = CompareColumnValue(i, key, i);
            if (std::is_neq(cmp))
                return cmp;
        }
        return std::partial_ordering::equivalent;
    }

    template <typename T>
    std::partial_ordering CompareNotNull(const ReplaceKeyTemplate<T> & key) const
    {
        for (int i = 0; i < Size(); ++i)
        {
            auto cmp = CompareColumnValueNotNull(i, key, i);
            if (std::is_neq(cmp))
                return cmp;
        }
        return std::partial_ordering::equivalent;
    }

    template <typename T>
    std::partial_ordering ComparePartNotNull(const ReplaceKeyTemplate<T> & key, int size) const
    {
        for (int i = 0; i < size; ++i)
        {
            auto cmp = CompareColumnValueNotNull(i, key, i);
            if (std::is_neq(cmp))
                return cmp;
        }
        return std::partial_ordering::equivalent;
    }

    template <typename T>
    std::partial_ordering CompareColumnValueNotNull(int column, const ReplaceKeyTemplate<T> & key, int keyColumn) const
    {
        return TypedCompare<true>(Column(column), position, key.Column(keyColumn), key.position);
    }

    template <typename T>
    std::partial_ordering CompareColumnValue(int column, const ReplaceKeyTemplate<T> & key, int keyColumn) const
    {
        return TypedCompare<false>(Column(column), position, key.Column(keyColumn), key.position);
    }

    int Size() const { return columns->size(); }
    int GetPosition() const { return position; }
    const arrow::Array & Column(int i) const { return *(*columns)[i]; }
    std::shared_ptr<arrow::Array> ColumnPtr(int i) const { return (*columns)[i]; }

    ReplaceKeyTemplate<const ArrayVec *> ToRaw() const
    {
        if constexpr (is_owning)
            return ReplaceKeyTemplate<const ArrayVec *>(columns.get(), position);
        else
            return *this;
    }

    template <typename T = ArrayVecPtr>
    requires is_owning
    std::shared_ptr<arrow::RecordBatch> RestoreBatch(const std::shared_ptr<arrow::Schema> & schema) const
    {
        return arrow::RecordBatch::Make(schema, columns[0]->length(), *columns);
    }

    template <typename T = ArrayVecPtr>
    requires is_owning
    std::shared_ptr<arrow::RecordBatch> ToBatch(const std::shared_ptr<arrow::Schema> & schema) const
    {
        auto batch = RestoreBatch(schema);
        return batch->Slice(position, 1);
    }

    template <typename T = ArrayVecPtr>
    requires is_owning
    static ReplaceKeyTemplate<ArrayVecPtr>
    FromBatch(const std::shared_ptr<arrow::RecordBatch> & batch, const std::shared_ptr<arrow::Schema> & key, int row)
    {
        ArrayVec columns;
        columns.reserve(key->num_fields());
        for (int i = 0; i < key->num_fields(); ++i)
        {
            auto & keyField = key->field(i);
            auto array = batch->GetColumnByName(keyField->name());
            columns.push_back(array);
        }

        return ReplaceKeyTemplate<ArrayVecPtr>(std::move(columns), row);
    }

    template <typename T = ArrayVecPtr>
    requires is_owning
    static ReplaceKeyTemplate<ArrayVecPtr> FromBatch(const std::shared_ptr<arrow::RecordBatch> & batch, int row)
    {
        auto columns = std::make_shared<ArrayVec>(batch->columns());
        return ReplaceKeyTemplate<ArrayVecPtr>(columns, row);
    }

    static ReplaceKeyTemplate<ArrayVecPtr> FromScalar(const std::shared_ptr<arrow::Scalar> & s)
    {
        auto res = arrow::MakeArrayFromScalar(*s, 1);
        return ReplaceKeyTemplate<ArrayVecPtr>(std::make_shared<ArrayVec>(1, *res), 0);
    }

    static std::shared_ptr<arrow::Scalar> ToScalar(const ReplaceKeyTemplate<ArrayVecPtr> & key, int colNumber = 0)
    {
        auto & column = key.Column(colNumber);
        auto res = column.GetScalar(key.GetPosition());
        return *res;
    }

private:
    ArrayVecPtr columns = nullptr;
    uint64_t position = 0;

    template <bool notNull>
    static std::partial_ordering TypedCompare(const arrow::Array & lhs, int lpos, const arrow::Array & rhs, int rpos)
    {
        arrow::Type::type typeId = lhs.type_id();
        switch (typeId)
        {
            case arrow::Type::UINT8:
                return CompareView<arrow::UInt8Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT8:
                return CompareView<arrow::Int8Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT16:
                return CompareView<arrow::UInt16Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT16:
                return CompareView<arrow::Int16Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT32:
                return CompareView<arrow::UInt32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT32:
                return CompareView<arrow::Int32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT64:
                return CompareView<arrow::UInt64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT64:
                return CompareView<arrow::Int64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::FLOAT:
                return CompareView<arrow::FloatArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DOUBLE:
                return CompareView<arrow::DoubleArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::STRING:
                return CompareView<arrow::StringArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::BINARY:
                return CompareView<arrow::BinaryArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::FIXED_SIZE_BINARY:
                return CompareView<arrow::FixedSizeBinaryArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::LARGE_BINARY:
                return CompareView<arrow::LargeBinaryArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::LARGE_STRING:
                return CompareView<arrow::LargeStringArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DATE32:
                return CompareView<arrow::Date32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DATE64:
                return CompareView<arrow::Date64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIMESTAMP:
                return CompareView<arrow::TimestampArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIME32:
                return CompareView<arrow::Time32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIME64:
                return CompareView<arrow::Time64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DURATION:
                return CompareView<arrow::DurationArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DECIMAL256:
                return CompareView<arrow::Decimal256Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DECIMAL:
                return CompareView<arrow::DecimalArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INTERVAL_MONTHS:
                return CompareView<arrow::MonthIntervalArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::NA:
            case arrow::Type::BOOL:
            case arrow::Type::HALF_FLOAT:
            case arrow::Type::INTERVAL_DAY_TIME:
            case arrow::Type::INTERVAL_MONTH_DAY_NANO:
            case arrow::Type::DENSE_UNION:
            case arrow::Type::DICTIONARY:
            case arrow::Type::EXTENSION:
            case arrow::Type::FIXED_SIZE_LIST:
            case arrow::Type::LARGE_LIST:
            case arrow::Type::LIST:
            case arrow::Type::MAP:
            case arrow::Type::MAX_ID:
            case arrow::Type::SPARSE_UNION:
            case arrow::Type::STRUCT:
            case arrow::Type::RUN_END_ENCODED:
                throw std::runtime_error("not implemented");
        }
        return std::partial_ordering::equivalent;
    }

    template <typename T, bool notNull>
    static std::partial_ordering CompareView(const arrow::Array & lhs, int lpos, const arrow::Array & rhs, int rpos)
    {
        auto & left = static_cast<const T &>(lhs);
        auto & right = static_cast<const T &>(rhs);
        if constexpr (notNull)
            return CompareValueNotNull(left.GetView(lpos), right.GetView(rpos));
        else
            return CompareValue(left.GetView(lpos), right.GetView(rpos), left.IsNull(lpos), right.IsNull(rpos));
    }

    template <typename T>
    static std::partial_ordering CompareValue(const T & x, const T & y, bool xIsNull, bool yIsNull)
    {
        // TODO: std::partial_ordering::unordered for both nulls?
        if (xIsNull)
            return std::partial_ordering::less;
        if (yIsNull)
            return std::partial_ordering::greater;
        return CompareValueNotNull(x, y);
    }

    template <typename T>
    static std::partial_ordering CompareValueNotNull(const T & x, const T & y)
    {
        if constexpr (std::is_same_v<T, std::string_view>)
        {
            size_t minSize = (x.size() < y.size()) ? x.size() : y.size();
            int cmp = memcmp(x.data(), y.data(), minSize);
            if (cmp < 0)
                return std::partial_ordering::less;
            else if (cmp > 0)
                return std::partial_ordering::greater;
            return CompareValueNotNull(x.size(), y.size());
        }
        else
        {
            return x <=> y;
        }
    }
};

using ReplaceKey = ReplaceKeyTemplate<std::shared_ptr<ArrayVec>>;
using RawReplaceKey = ReplaceKeyTemplate<const ArrayVec *>;

}
