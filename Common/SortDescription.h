#pragma once

#include <vector>
#include <cstddef>
#include <string>

namespace AH
{

/// Description of the sorting rule by one column.
struct SortColumnDescription
{
    std::string column_name; /// The name of the column.
    int direction = 1;       /// 1 - ascending, -1 - descending.
#if 0 // TODO
    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.
                             /// To achieve NULLS LAST, set it equal to direction, to achieve NULLS FIRST, set it opposite.
#endif
};

/// Description of the sorting rule for several columns.
using SortDescription = std::vector<SortColumnDescription>;

}
