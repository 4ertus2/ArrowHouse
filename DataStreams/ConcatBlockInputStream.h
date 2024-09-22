#pragma once

#include <DataStreams/IBlockInputStream.h>

namespace AH
{

/** Combines several sources into one.
  * Unlike UnionBlockInputStream, it does this sequentially.
  * Blocks of different sources are not interleaved with each other.
  */
class ConcatBlockInputStream : public IBlockInputStream
{
public:
    ConcatBlockInputStream(InputStreams inputs_)
    {
        children.insert(children.end(), inputs_.begin(), inputs_.end());
        current_stream = children.begin();
    }

    Header getHeader() const override { return IBlockInputStream::getHeader(children.at(0)); }

    /// We call readSuffix prematurely by ourself. Suppress default behaviour.
    void readSuffix() override { }

protected:
    Block readImpl() override
    {
        Block res;

        while (current_stream != children.end())
        {
            res = (*current_stream)->read();

            if (res)
                break;
            else
            {
                (*current_stream)->readSuffix();
                ++current_stream;
            }
        }

        return res;
    }

private:
    InputStreams::iterator current_stream;
};

}
