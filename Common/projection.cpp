#include <Common/projection.h>

namespace AH
{

Header projection(const Header & src_schema, const Names & column_names, bool throw_if_column_not_found)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(column_names.size());
    for (auto & name : column_names)
    {
        int pos = src_schema->GetFieldIndex(name);
        if (pos < 0)
        {
            if (throw_if_column_not_found)
                throw std::runtime_error("no column " + name + " in batch " + src_schema->ToString());
            continue;
        }
        fields.push_back(src_schema->field(pos));
    }

    return std::make_shared<arrow::Schema>(std::move(fields));
}

static int columnIndexByName(const Header & schema, const std::string & name)
{
    return schema->GetFieldIndex(name);
}

static int columnIndexByName(const Header & schema, std::shared_ptr<arrow::Field> field)
{
    return schema->GetFieldIndex(field->name());
}

template <typename T>
static Block projectionImpl(const Block & src_batch, const std::vector<T> & column_names, bool throw_if_column_not_found)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(column_names.size());
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(column_names.size());

    auto src_schema = src_batch->schema();
    for (auto & name : column_names)
    {
        int pos = columnIndexByName(src_schema, name);
        if (pos < 0)
        {
            if (throw_if_column_not_found)
                throw std::runtime_error("no column in block " + src_schema->ToString());
            continue; // TODO: check no column expected
        }
        fields.push_back(src_schema->field(pos));
        columns.push_back(src_batch->column(pos));
    }

    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), src_batch->num_rows(), std::move(columns));
}

Block projection(const Block & src_batch, const Names & dst_schema, bool throw_if_column_not_found)
{
    return projectionImpl(src_batch, dst_schema, throw_if_column_not_found);
}

Block projection(const Block & src_batch, const Header & dst_schema, bool throw_if_column_not_found)
{
    return projectionImpl(src_batch, dst_schema->fields(), throw_if_column_not_found);
}

}
