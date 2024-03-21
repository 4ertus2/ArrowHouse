// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include <arrow/api.h>

namespace CH
{

/// Description of the sorting rule for several columns.
struct SortDescription
{
    /// @note In case you have PK and snapshot column you should sort with {ASC PK, DESC snap} key and replase with PK
    std::shared_ptr<arrow::Schema> sorting_key;
    std::shared_ptr<arrow::Schema> replace_key; /// Keep first visited (sorting_key ordered) of dups
    std::vector<int> directions; /// 1 - ascending, -1 - descending.
    bool not_null{false};
    bool reverse{false}; // Read sources from bottom to top. With inversed directions leads to DESC dst for ASC src

    SortDescription() = default;

    SortDescription(const std::shared_ptr<arrow::Schema> & sortingKey, const std::shared_ptr<arrow::Schema> & replaceKey = {})
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
