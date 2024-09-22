// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <DataStreams/IBlockStream_fwd.h>


namespace AH
{

/** Interface of stream for writing data
  */
class IOutputStream
{
public:
    IOutputStream() { }
    virtual ~IOutputStream() { }

    IOutputStream(const IOutputStream &) = delete;
    IOutputStream & operator=(const IOutputStream &) = delete;

    /** Write block.
      */
    virtual void write(const Clod & block) = 0;

    /** Write or do something before all data or after all data.
      */
    virtual void writePrefix() { }
    virtual void writeSuffix() { }

    /** Flush output buffers if any.
      */
    virtual void flush() { }
};


class IBlockOutputStream : public IOutputStream
{
public:
    IBlockOutputStream() { }

    /** Get data structure of the stream in a form of "header" block (it is also called "sample block").
      * Header block contains column names, data types, columns of size 0. Constant columns must have corresponding values.
      * You must pass blocks of exactly this structure to the 'write' method.
      */
    virtual Header getHeader() const = 0;

    static Header getHeader(const OutputStreamPtr & stream) { return std::static_pointer_cast<IBlockOutputStream>(stream)->getHeader(); }
};

}
