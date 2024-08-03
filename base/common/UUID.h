#pragma once

#include <common/strong_typedef.h>
#include <common/extended_types.h>

namespace AH
{
    using UUID = StrongTypedef<UInt128, struct UUIDTag>;
}
