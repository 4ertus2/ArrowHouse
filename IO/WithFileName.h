#pragma once
#include <common/types.h>

namespace AH
{

class ReadBuffer;

class WithFileName
{
public:
    virtual String getFileName() const = 0;
    virtual ~WithFileName() = default;
};

String getFileNameFromReadBuffer(const ReadBuffer & in);
String getExceptionEntryWithFileName(const ReadBuffer & in);

}
