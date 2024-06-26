#pragma once
#include <arrow/api.h>

namespace AHY
{

template <typename T>
void Validate(const T &)
{
}

template <typename TType>
struct TTypeWrapper
{
    using T = TType;
};

template <class TResult, TResult defaultValue, typename TFunc, bool EnableNull = false>
TResult SwitchTypeImpl(arrow::Type::type typeId, TFunc && f)
{
    switch (typeId)
    {
        case arrow::Type::NA: {
            if constexpr (EnableNull)
                return f(TTypeWrapper<arrow::NullType>());
            break;
        }
        case arrow::Type::BOOL:
            return f(TTypeWrapper<arrow::BooleanType>());
        case arrow::Type::UINT8:
            return f(TTypeWrapper<arrow::UInt8Type>());
        case arrow::Type::INT8:
            return f(TTypeWrapper<arrow::Int8Type>());
        case arrow::Type::UINT16:
            return f(TTypeWrapper<arrow::UInt16Type>());
        case arrow::Type::INT16:
            return f(TTypeWrapper<arrow::Int16Type>());
        case arrow::Type::UINT32:
            return f(TTypeWrapper<arrow::UInt32Type>());
        case arrow::Type::INT32:
            return f(TTypeWrapper<arrow::Int32Type>());
        case arrow::Type::UINT64:
            return f(TTypeWrapper<arrow::UInt64Type>());
        case arrow::Type::INT64:
            return f(TTypeWrapper<arrow::Int64Type>());
        case arrow::Type::HALF_FLOAT:
            return f(TTypeWrapper<arrow::HalfFloatType>());
        case arrow::Type::FLOAT:
            return f(TTypeWrapper<arrow::FloatType>());
        case arrow::Type::DOUBLE:
            return f(TTypeWrapper<arrow::DoubleType>());
        case arrow::Type::STRING:
            return f(TTypeWrapper<arrow::StringType>());
        case arrow::Type::BINARY:
            return f(TTypeWrapper<arrow::BinaryType>());
        case arrow::Type::FIXED_SIZE_BINARY:
            return f(TTypeWrapper<arrow::FixedSizeBinaryType>());
        case arrow::Type::DATE32:
            return f(TTypeWrapper<arrow::Date32Type>());
        case arrow::Type::DATE64:
            return f(TTypeWrapper<arrow::Date64Type>());
        case arrow::Type::TIMESTAMP:
            return f(TTypeWrapper<arrow::TimestampType>());
        case arrow::Type::TIME32:
            return f(TTypeWrapper<arrow::Time32Type>());
        case arrow::Type::TIME64:
            return f(TTypeWrapper<arrow::Time64Type>());
        case arrow::Type::INTERVAL_MONTHS:
            return f(TTypeWrapper<arrow::MonthIntervalType>());
        case arrow::Type::DECIMAL:
            return f(TTypeWrapper<arrow::Decimal128Type>());
        case arrow::Type::DURATION:
            return f(TTypeWrapper<arrow::DurationType>());
        case arrow::Type::LARGE_STRING:
            return f(TTypeWrapper<arrow::LargeStringType>());
        case arrow::Type::LARGE_BINARY:
            return f(TTypeWrapper<arrow::LargeBinaryType>());
        case arrow::Type::DECIMAL256:
        case arrow::Type::DENSE_UNION:
        case arrow::Type::DICTIONARY:
        case arrow::Type::EXTENSION:
        case arrow::Type::FIXED_SIZE_LIST:
        case arrow::Type::INTERVAL_DAY_TIME:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::LIST:
        case arrow::Type::MAP:
        case arrow::Type::MAX_ID:
        case arrow::Type::SPARSE_UNION:
        case arrow::Type::STRUCT:
        case arrow::Type::INTERVAL_MONTH_DAY_NANO:
        case arrow::Type::RUN_END_ENCODED:
        case arrow::Type::STRING_VIEW:
        case arrow::Type::BINARY_VIEW:
        case arrow::Type::LIST_VIEW:
        case arrow::Type::LARGE_LIST_VIEW:
            break;
    }

    return defaultValue;
}

template <typename TFunc, bool EnableNull = false>
bool SwitchType(arrow::Type::type typeId, TFunc && f)
{
    return SwitchTypeImpl<bool, false, TFunc, EnableNull>(typeId, std::move(f));
}

template <typename TFunc>
bool SwitchTypeWithNull(arrow::Type::type typeId, TFunc && f)
{
    return SwitchType<TFunc, true>(typeId, std::move(f));
}

template <typename TFunc>
bool SwitchArrayType(const arrow::Datum & column, TFunc && f)
{
    auto type = column.type();
    return SwitchType(type->id(), std::forward<TFunc>(f));
}

template <typename T>
bool Append(arrow::ArrayBuilder & builder, const typename T::c_type & value)
{
    using TBuilder = typename arrow::TypeTraits<T>::BuilderType;

    Validate(static_cast<TBuilder &>(builder).Append(value));
    return true;
}

template <typename T>
bool Append(arrow::ArrayBuilder & builder, std::string_view value)
{
    using TBuilder = typename arrow::TypeTraits<T>::BuilderType;

    Validate(static_cast<TBuilder &>(builder).Append(value));
    return true;
}

template <typename T>
bool Append(arrow::ArrayBuilder & builder, const typename T::c_type * values, size_t size)
{
    using TBuilder = typename arrow::NumericBuilder<T>;

    Validate(static_cast<TBuilder &>(builder).AppendValues(values, size));
    return true;
}

template <typename T>
bool Append(arrow::ArrayBuilder & builder, const std::vector<typename T::c_type> & values)
{
    using TBuilder = typename arrow::NumericBuilder<T>;

    Validate(static_cast<TBuilder &>(builder).AppendValues(values.data(), values.size()));
    return true;
}

template <typename T>
bool Append(T & builder, const arrow::Array & array, int position, uint64_t * recordSize = nullptr)
{
    return SwitchType(
        array.type_id(),
        [&](const auto & type)
        {
            using TWrap = std::decay_t<decltype(type)>;
            using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
            using TBuilder = typename arrow::TypeTraits<typename TWrap::T>::BuilderType;

            auto & typedArray = static_cast<const TArray &>(array);
            auto & typedBuilder = static_cast<TBuilder &>(builder);

            if (typedArray.IsNull(position))
            {
                Validate(typedBuilder.AppendNull());
                if (recordSize)
                    *recordSize += 4;
                return true;
            }
            else
            {
                if constexpr (!arrow::has_string_view<typename TWrap::T>::value)
                {
                    Validate(typedBuilder.Append(typedArray.GetView(position)));
                    if (recordSize)
                        *recordSize += sizeof(typedArray.GetView(position));
                    return true;
                }
                if constexpr (arrow::has_string_view<typename TWrap::T>::value)
                {
                    Validate(typedBuilder.Append(typedArray.GetView(position)));
                    if (recordSize)
                        *recordSize += typedArray.GetView(position).size();
                    return true;
                }
            }
            throw std::runtime_error("unpredictable variant");
            return false;
        });
}

}
