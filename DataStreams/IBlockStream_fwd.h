#pragma once

#include <memory>
#include <vector>

namespace AH
{

class IInputStream;
class IOutputStream;

using InputStreamPtr = std::shared_ptr<IInputStream>;
using InputStreams = std::vector<InputStreamPtr>;
using OutputStreamPtr = std::shared_ptr<IOutputStream>;
using OutputStreams = std::vector<OutputStreamPtr>;

}
