#include <DataStreams/FilterColumnsBlockInputStream.h>

namespace AH
{

Header projection(const Header & src_schema, const Names & column_names, bool throw_if_column_not_found)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(column_names.size());
    for (auto & name : column_names)
    {
        int pos = src_schema->GetFieldIndex(name);
        if (pos < 0) {
            if (throw_if_column_not_found)
                throw std::runtime_error("no column " + name + " in batch " + src_schema->ToString());
            continue;
        }
        fields.push_back(src_schema->field(pos));
    }

    return std::make_shared<arrow::Schema>(std::move(fields));
}

Block projection(const Block & src_batch, const Names & column_names, bool throw_if_column_not_found)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(column_names.size());
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(column_names.size());

    auto src_schema = src_batch->schema();
    for (auto & name : column_names)
    {
        int pos = src_schema->GetFieldIndex(name);
        if (pos < 0) {
            if (throw_if_column_not_found)
                throw std::runtime_error("no column " + name + " in block " + src_schema->ToString());
            continue; // TODO: check no column expected
        }
        fields.push_back(src_schema->field(pos));
        columns.push_back(src_batch->column(pos));
    }

    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), src_batch->num_rows(), std::move(columns));
}

Header FilterColumnsBlockInputStream::getHeader() const
{
    Header header = children.back()->getHeader();
    return projection(header, columns_to_save, throw_if_column_not_found);
}

Block FilterColumnsBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block)
        return block;

    return projection(block, columns_to_save, throw_if_column_not_found);
}

}
