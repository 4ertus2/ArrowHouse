#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace CH
{

/// Empty stream of blocks of specified structure.
class NullBlockInputStream : public IBlockInputStream
{
public:
    NullBlockInputStream(const Header & header_) : header(header_) {}

    Header getHeader() const override { return header; }
    String getName() const override { return "Null"; }

private:
    Header header;

    Block readImpl() override { return {}; }
};

}
