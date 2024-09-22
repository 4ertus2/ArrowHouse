#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace AHY
{

using namespace AH;

struct Program;

/** Executes a certain expression over the block.
  * The expression consists of column identifiers from the block, constants, common functions.
  * For example: hits * 2 + 3, url LIKE '%yandex%'
  * The expression processes each row independently of the others.
  */
class ExpressionBlockInputStream : public IBlockInputStream
{
public:
    using ProgramPtr = std::shared_ptr<Program>;

    ExpressionBlockInputStream(const InputStreamPtr & input, ProgramPtr ssa_);

    Header getHeader() const override { return cached_header; }

protected:
    Block readImpl() override;

private:
    ProgramPtr ssa;
    Header cached_header;
};

}
