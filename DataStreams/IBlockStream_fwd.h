#pragma once

#include <memory>
#include <vector>

namespace arrow
{
class RecordBatch;
}

namespace AH
{

template <typename T>
class IInputStream;
template <typename T>
class IOutputStream;

using InputStreamPtr = std::shared_ptr<IInputStream<arrow::RecordBatch>>;
using InputStreams = std::vector<InputStreamPtr>;
using OutputStreamPtr = std::shared_ptr<IOutputStream<arrow::RecordBatch>>;
using OutputStreams = std::vector<OutputStreamPtr>;

//using StubInputStreamPtr = std::shared_ptr<IInputStream<Stub>>;
//using StubOutputStreamPtr = std::shared_ptr<IOutputStream<Stub>>;

}
