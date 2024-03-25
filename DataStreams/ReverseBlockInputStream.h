#pragma once

#include <DataStreams/IBlockInputStream.h>

namespace CH
{

/// Reverses an order of rows in every block in a data stream.
class ReverseBlockInputStream : public IBlockInputStream
{
public:
    ReverseBlockInputStream(const BlockInputStreamPtr & input);

    String getName() const override { return "Reverse"; }
    Header getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;
};

}
