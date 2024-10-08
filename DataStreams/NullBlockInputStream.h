#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace AH
{

/// Empty stream of blocks of specified structure.
class NullBlockInputStream : public IBlockInputStream
{
public:
    NullBlockInputStream(const Header & header_) : header(header_) {}

    Header getHeader() const override { return header; }

private:
    Header header;

    Block readImpl() override { return {}; }
};

}
