// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Aggregator.h>
#include "arrow_clickhouse_types.h"


namespace AH
{

/** A pre-aggregate stream of blocks in which each block is already aggregated.
  * Aggregate functions in blocks should not be finalized so that their states can be merged.
  */
class MergingAggregatedBlockInputStream : public IBlockInputStream
{
public:
    MergingAggregatedBlockInputStream(const InputStreamPtr & input, const Aggregator::Params & params, bool final_)
        : aggregator(params), final(final_)
    {
        children.push_back(input);
    }

    Header getHeader() const override;

protected:
    Block readImpl() override;

private:
    Aggregator aggregator;
    bool final;

    bool executed = false;
    BlocksList blocks;
    BlocksList::iterator it;
};

}
