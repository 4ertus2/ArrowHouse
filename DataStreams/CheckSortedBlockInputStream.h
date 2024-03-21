#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <YdbModes/ReplaceKey.h>
#include <YdbModes/SortDescription.h>

#include <optional>

namespace CH
{

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
    std::optional<ReplaceKey> last_row;
};
}
