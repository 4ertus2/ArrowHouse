#pragma once

#include <arrow_clickhouse_types.h>

namespace AH
{

Header projection(const Header & src_schema, const Names & column_names, bool throw_if_column_not_found);
Block projection(const Block & src_batch, const Names & column_names, bool throw_if_column_not_found);
Block projection(const Block & src_batch, const Header & dst_schema, bool throw_if_column_not_found);

}
