#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Common/SortDescription.h>


namespace AHY
{

using namespace AH;

class SortingBlockInputStream : public IBlockInputStream
{
public:
    SortingBlockInputStream(const BlockInputStreamPtr & input_, const SortDescription & description_)
        : description(description_)
    {
        for (auto & col_descr : description)
            if (col_descr.direction != 1)
                throw std::runtime_error("sort directions are not supported");
        children.push_back(input_);
    }

    String getName() const override { return "Sorting"; }
    Header getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    SortDescription description;
};

}
