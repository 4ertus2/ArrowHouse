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
            sink.getOutput(thread_num)->write(block);
        }

        void onFinishThread(size_t /*thread_num*/) { }
        void onFinish() { }

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

    static void copyNToOne(
        const BlockInputStreams & inputs,
        BlockOutputStreamPtr output,
        uint32_t max_compute_threads = 1,
        uint32_t max_io_threads = 0,
        ProgressCallback progress = {})
    {
        BlockOutputStreamPtr mt_output = std::make_shared<GuardedBlockOutputStream>(output);
        mt_output->writePrefix();

        constexpr uint32_t flags = ParallelInput::PREFIX | ParallelInput::SUFFIX;
        ParallelInputsSink sink(inputs, mt_output, max_compute_threads, max_io_threads, flags, progress);
        sink.process();
        sink.finalize();

        mt_output->writeSuffix();
    }

    static void copyNToN(const BlockInputStreams & inputs, BlockOutputStreams & outputs, ProgressCallback progress = {})
    {
        for (auto & output : outputs)
            output->writePrefix();

        constexpr uint32_t flags = ParallelInput::PREFIX | ParallelInput::SUFFIX | ParallelInput::AFFINITY;
        ParallelInputsSink sink(inputs, outputs, flags, progress);
        sink.process();
        sink.finalize();

        for (auto & output : outputs)
            output->writeSuffix();
    }

    ParallelInputsSink(
        const BlockInputStreams & inputs,
        BlockOutputStreamPtr mt_output,
        uint32_t max_compute_threads,
        uint32_t max_io_threads = 0,
        uint32_t flags = 0,
        ProgressCallback progress = {})
        : handler(*this, progress), outputs({mt_output}), processor(inputs, max_compute_threads, max_io_threads, handler, flags)
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

    BlockOutputStreamPtr & getOutput(unsigned thread_num) { return (outputs.size() == 1) ? outputs[0] : outputs[thread_num]; }

private:
    Handler handler;
    BlockOutputStreams outputs;
    ParallelInputsProcessor<Handler> processor;
    std::exception_ptr exception;

    ParallelInputsSink(const BlockInputStreams & inputs, BlockOutputStreams outputs, uint32_t flags, ProgressCallback progress = {})
        : handler(*this, progress), outputs(outputs), processor(inputs, inputs.size(), 0, handler, flags)
    {
        if (inputs.size() != outputs.size())
            throw std::runtime_error("not expected");
    }
};

}
