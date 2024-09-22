#pragma once

#include <DataStreams/IBlockInputStream.h>

namespace AH
{

/// Reverses an order of rows in every block in a data stream.
class ReverseBlockInputStream : public IBlockInputStream
{
public:
    ReverseBlockInputStream(const InputStreamPtr & input);

    Header getHeader() const override { return IBlockInputStream::getHeader(children.at(0)); }

protected:
    Block readImpl() override;
};

}
