#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <YdbModes/CompositeKey.h>
#include <Common/SortDescription.h>

#include <optional>

namespace AHY
{

using namespace AH;

/// Streams checks that flow of blocks is sorted in the sort_description order
/// Othrewise throws exception in readImpl function.
class CheckSortedBlockInputStream : public IBlockInputStream
{
public:
    CheckSortedBlockInputStream(const BlockInputStreamPtr & input_, const SortDescription & sort_description_);

    String getName() const override { return "CheckSorted"; }
    Header getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    Header header;
    SortDescription sort_description;
    std::optional<CompositeKey> last_row;
};
}
