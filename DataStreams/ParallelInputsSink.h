#pragma once

#include <exception>

#include <DataStreams/GuardedBlockOutputStream.h>
#include <DataStreams/ParallelInputsProcessor.h>


namespace AH
{

class ParallelInputsSink
{
public:
    using ProgressCallback = std::function<void(const Block & block, unsigned thread_num)>;

    class Handler
    {
    public:
        Handler(ParallelInputsSink & sink_, ProgressCallback progress_) : sink(sink_), progress(progress_) { }

        void onBlock(Block && block, unsigned thread_num)
        {
            if (progress)
                progress(block, thread_num);
            sink.getOutput(thread_num).write(block);
        }

        bool onResetStream(size_t /*thread_num*/)
        {
            if (auto affinity = sink.processor.getAffinity())
            {
                size_t current = affinity->get();
                if (current < sink.outputs.size())
                    sink.outputs[current].destroy();

                size_t next = affinity->setNext();
                if (next < sink.outputs.size())
                    return true;
            }
            return false;
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
        const InputStreams & inputs,
        OutputStreamPtr output,
        uint32_t max_compute_threads = 1,
        uint32_t max_io_threads = 0,
        ProgressCallback progress = {})
    {
        OutputStreamPtr mt_output = std::make_shared<GuardedBlockOutputStream>(output);

        constexpr uint32_t flags = ParallelInput::PREFIX | ParallelInput::SUFFIX;
        ParallelInputsSink sink(inputs, mt_output, max_compute_threads, max_io_threads, flags, progress);
        sink.process();
        sink.finalize();
    }

    static void copyNToN(const InputStreams & inputs, OutputStreams & outputs, unsigned max_threads = 0, ProgressCallback progress = {})
    {
        constexpr uint32_t flags = ParallelInput::PREFIX | ParallelInput::SUFFIX | ParallelInput::AFFINITY;
        max_threads = max_threads ? max_threads : inputs.size();
        ParallelInputsSink sink(inputs, outputs, max_threads, flags, progress);
        sink.process();
        sink.finalize();
    }

    ParallelInputsSink(
        const InputStreams & inputs,
        OutputStreamPtr mt_output,
        uint32_t max_compute_threads,
        uint32_t max_io_threads = 0,
        uint32_t flags = 0,
        ProgressCallback progress = {})
        : handler(*this, progress), processor(inputs, max_compute_threads, max_io_threads, handler, flags)
    {
        outputs.emplace_back(ParallelOutput(mt_output, flags));
    }

    void process() { processor.process(); }
    void cancel(bool kill) { processor.cancel(kill); }

    void finalize()
    {
        processor.wait();
        if (exception)
            std::rethrow_exception(exception);
    }

    ParallelOutput & getOutput(unsigned thread_num)
    {
        if (outputs.size() == 1)
            return outputs[0];
        if (auto affinity = processor.getAffinity())
            return outputs[affinity->get()];
        return outputs[thread_num];
    }

private:
    Handler handler;
    std::vector<ParallelOutput> outputs;
    ParallelInputsProcessor<Handler> processor;
    std::exception_ptr exception;

    ParallelInputsSink(
        const InputStreams & inputs, OutputStreams & outputs_, unsigned max_threads, uint32_t flags, ProgressCallback progress = {})
        : handler(*this, progress), processor(inputs, max_threads, max_threads, handler, flags)
    {
        outputs.reserve(outputs_.size());
        for (auto & stream : outputs_)
            outputs.emplace_back(ParallelOutput(stream, flags));

        if (inputs.size() != outputs.size())
            throw std::runtime_error(__FUNCTION__);
    }
};

}
