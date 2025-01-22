#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IBlockStream_fwd.h>

#include <mutex>

namespace AH
{

class GuardedBlockOutputStream : public IBlockOutputStream
{
public:
    GuardedBlockOutputStream(OutputStreamPtr stream_) : stream(stream_) { }

    Header getHeader() const override
    {
        std::lock_guard lock(mutex);
        return IBlockOutputStream::getHeader(stream);
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
    OutputStreamPtr stream;
    mutable std::mutex mutex;
};

}