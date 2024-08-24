#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IBlockStream_fwd.h>

#include <mutex>

namespace AH
{

class GuardedBlockOutputStream : public IBlockOutputStream
{
public:
    GuardedBlockOutputStream(BlockOutputStreamPtr stream_) : stream(stream_) { }

    Block getHeader() const override
    {
        std::lock_guard lock(mutex);
        return stream->getHeader();
    }

    void write(const Block & block) override
    {
        std::lock_guard lock(mutex);
        stream->write(block);
    }

    void writePrefix() override
    {
        std::lock_guard lock(mutex);
        stream->writePrefix();
    }

    void writeSuffix() override
    {
        std::lock_guard lock(mutex);
        stream->writeSuffix();
    }

    void flush() override
    {
        std::lock_guard lock(mutex);
        stream->flush();
    }

private:
    BlockOutputStreamPtr stream;
    mutable std::mutex mutex;
};

}