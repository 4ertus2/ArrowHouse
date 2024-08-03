#pragma once

#include <IO/ReadBuffer.h>

namespace AH
{

class ReadBufferWrapperBase
{
public:
    virtual const ReadBuffer & getWrappedReadBuffer() const = 0;
    virtual ~ReadBufferWrapperBase() = default;
};

}
