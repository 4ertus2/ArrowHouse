#pragma once

#include <exception>

#include <DataStreams/GuardedBlockOutputStream.h>
#include <DataStreams/ParallelInputsProcessor.h>


namespace AH
{


class ParallelInputsSink
{
public:
    using ProgressCallback = std::function<void(const Block & clock, unsigned thread_num)>;

    class Handler
    {
    public:
        Handler(ParallelInputsSink & sink_, ProgressCallback progress_) : sink(sink_), progress(progress_) { }

        void onBlock(Block && block, unsigned thread_num)
        {
            if (progress)
                progress(block, thread_num);
            sink.output->write(block);
        }

        void onFinishThread(size_t /*thread_num*/) { }
        void onFinish() { sink.output->flush(); }

        void onException(std::exception_ptr & ex, unsigned /*thread_num*/)
        {
            if (!sink.exception)
                sink.exception = ex;
            sink.cancel(false);
        }

    private:
        ParallelInputsSink & sink;
        ProgressCallback progress;
    };

    static void copyData(
        const BlockInputStreams & inputs,
        BlockOutputStreamPtr output,
        uint32_t max_compute_threads = 1,
        uint32_t max_io_threads = 0,
        ProgressCallback progress = {})
    {
        BlockOutputStreamPtr mt_output = std::make_shared<GuardedBlockOutputStream>(output);
        mt_output->writePrefix();

        constexpr uint32_t flags = ParallelInputsStream::PREFIX | ParallelInputsStream::SUFFIX;
        ParallelInputsSink sink(inputs, mt_output, max_compute_threads, max_io_threads, flags, progress);
        sink.process();
        sink.finalize();

        mt_output->writeSuffix();
    }

    ParallelInputsSink(
        const BlockInputStreams & inputs,
        BlockOutputStreamPtr mt_output,
        uint32_t max_compute_threads,
        uint32_t max_io_threads = 0,
        uint32_t flags = 0,
        ProgressCallback progress = {})
        : handler(*this, progress), output(mt_output), processor(inputs, max_compute_threads, max_io_threads, handler, flags)
    {
    }

    void process() { processor.process(); }
    void cancel(bool kill) { processor.cancel(kill); }

    void finalize()
    {
        processor.wait();
        if (exception)
            std::rethrow_exception(exception);
    }

private:
    Handler handler;
    BlockOutputStreamPtr output;
    ParallelInputsProcessor<Handler> processor;
    std::exception_ptr exception;
};

}
