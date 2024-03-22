#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <YdbModes/SortDescription.h>


namespace CHY
{

using namespace CH;

class SortingBlockInputStream : public IBlockInputStream
{
public:
    SortingBlockInputStream(const BlockInputStreamPtr & input_, std::shared_ptr<SortDescription> description_)
        : description(description_)
    {
        for (auto & dir : description->directions)
            if (dir != 1)
                throw std::runtime_error("sort directions are not supported");
        children.push_back(input_);
    }

    String getName() const override { return "Sorting"; }
    Header getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    std::shared_ptr<SortDescription> description;
};

}
