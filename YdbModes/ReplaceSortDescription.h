#pragma once
#include <arrow/api.h>
#include <Common/SortDescription.h>

namespace AHY
{

/// Description of the sorting rule for several columns.
struct ReplaceSortDescription
{
    /// @note In case you have PK and snapshot column you should sort with {ASC PK, DESC snap} key and replase with PK
    std::shared_ptr<arrow::Schema> sorting_key;
    std::shared_ptr<arrow::Schema> replace_key; /// Keep first visited (sorting_key ordered) of dups
    std::vector<int> directions; /// 1 - ascending, -1 - descending.
    bool not_null{false};
    bool reverse{false}; // Read sources from bottom to top. With inversed directions leads to DESC dst for ASC src

    ReplaceSortDescription(const AH::SortDescription & sort_desrc, const std::shared_ptr<arrow::Schema> & schema)
    {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(sort_desrc.size());
        directions.reserve(sort_desrc.size());

        for (auto & col_descr : sort_desrc)
        {
            fields.push_back(schema->GetFieldByName(col_descr.column_name));
            directions.push_back(col_descr.direction);
        }
        sorting_key = std::make_shared<arrow::Schema>(fields);
    }

    ReplaceSortDescription(const std::shared_ptr<arrow::Schema> & sortingKey, const std::shared_ptr<arrow::Schema> & replaceKey = {})
        : sorting_key(sortingKey), replace_key(replaceKey), directions(sortingKey->num_fields(), 1)
    {
    }

    size_t Size() const { return sorting_key->num_fields(); }
    int Direction(size_t pos) const { return directions[pos]; }
    bool Replace() const { return replace_key.get(); }

    void Inverse()
    {
        reverse = !reverse;
        for (int & dir : directions)
            dir *= -1;
    }
};

}
