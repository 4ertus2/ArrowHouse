#include <YdbModes/ExpressionBlockInputStream.h>
#include <YdbModes/SsaProgram.h>


namespace AHY
{

ExpressionBlockInputStream::ExpressionBlockInputStream(const InputStreamPtr & input, ProgramPtr ssa_) : ssa(ssa_)
{
    children.push_back(input);

    auto schema = IBlockInputStream::getHeader(children.back());
    auto empty_batch = *arrow::RecordBatch::MakeEmpty(schema);
    applyProgram(empty_batch, *ssa).ok();
    cached_header = empty_batch->schema();
}

Block ExpressionBlockInputStream::readImpl()
{
    Block res = children.back()->read();
    if (res)
        applyProgram(res, *ssa).ok();
    return res;
}

}
