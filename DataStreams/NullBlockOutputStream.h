// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once

#include <DataStreams/IBlockOutputStream.h>


namespace AH
{

class NullBlockOutputStream : public IBlockOutputStream
{
public:
    NullBlockOutputStream(const Header & header_) : header(header_) { }
    Header getHeader() const override { return header; }
    void write(const Clod &) override { }

private:
    Header header;
};

}
