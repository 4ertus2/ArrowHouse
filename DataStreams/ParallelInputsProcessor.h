#pragma once

#include <atomic>
#include <exception>
#include <mutex>
#include <queue>
#include <thread>

#include <DataStreams/GuardedBlockOutputStream.h>
#include <DataStreams/IBlockInputStream.h>


/** Allows to process multiple block input streams (sources) in parallel, using specified number of threads.
  * Reads (pulls) blocks from any available source and passes it to specified handler.
  *
  * Implemented in following way:
  * - there are multiple input sources to read blocks from;
  * - there are multiple threads, that could simultaneously read blocks from different sources;
  * - "available" sources (that are not read in any thread right now) are put in queue of sources;
  * - when thread take a source to read from, it removes source from queue of sources,
  *    then read block from source and then put source back to queue of available sources.
  */

namespace AH
{

class ParallelInputsStream : public IBlockInputStream
{
public:
    enum Flags
    {
        NONE = 0,
        PREFIX = 0x1,
        SUFFIX = 0x2,
    };

    struct Input
    {
        BlockInputStreamPtr stream;
        uint32_t flags = NONE;
        size_t i = 0;

        Input(const BlockInputStreamPtr & in = {}, uint32_t flags_ = 0, size_t i_ = 0) : stream(in), flags(flags_), i(i_) { }

        Block read()
        {
            if (flags & PREFIX)
            {
                flags &= ~PREFIX;
                stream->readPrefix();
            }

            Block block = stream->read();

            if (flags & SUFFIX)
            {
                flags &= ~SUFFIX;
                stream->readSuffix();
            }
            return block;
        }
    };

    ParallelInputsStream(const BlockInputStreams & inputs_, uint32_t flags = NONE)
    {
        if (inputs_.empty())
            throw std::runtime_error("unexpected empty inputs for ParallelInputsStream");

        children = inputs_;

        for (size_t i = 0; i < inputs_.size(); ++i)
            available_inputs.emplace(inputs_[i], flags, i);
    }

    String getName() const override { return "ParallelInputsStream"; }
    Header getHeader() const override { return children[0]->getHeader(); }

    Block readImpl() override
    {
        Input input = popInput();
        if (!input.stream)
            return {};

        if (Block block = input.read())
        {
            pushInput(std::move(input));
            return block;
        }

        if (isCancelled())
            return {};
        return read();
    }

private:
    std::queue<Input> available_inputs;
    std::mutex available_inputs_mutex;

    Input popInput()
    {
        std::lock_guard lock(available_inputs_mutex);

        if (available_inputs.empty())
            return {};

        Input input = available_inputs.front();
        available_inputs.pop();
        return input;
    }

    void pushInput(Input && input)
    {
        std::lock_guard lock(available_inputs_mutex);

        available_inputs.emplace(std::move(input));
    }
};

/// Example of the handler.
struct ParallelInputsHandler
{
    /// Processing the data block.
    void onBlock(Block && /*block*/, unsigned /*thread_num*/) { }

    /// Called for each thread, when the thread has nothing else to do.
    /// Due to the fact that part of the sources has run out, and now there are fewer sources left than streams.
    /// Called if the `onException` method does not throw an exception; is called before the `onFinish` method.
    void onFinishThread(unsigned /*thread_num*/) { }

    /// Blocks are over. Due to the fact that all sources ran out or because of the cancellation of work.
    /// This method is always called exactly once, at the end of the work, if the `onException` method does not throw an exception.
    void onFinish() { }

    /// Exception handling. It is reasonable to call the ParallelInputsProcessor::cancel method in this method, and also pass the exception to the main thread.
    void onException(std::exception_ptr & /*exception*/, unsigned /*thread_num*/) { }
};


template <typename Handler>
class ParallelInputsProcessor
{
public:
    ParallelInputsProcessor(const BlockInputStreams & inputs_, unsigned max_threads_, Handler & handler_, uint32_t flags = 0)
        : input(std::make_shared<ParallelInputsStream>(inputs_, flags)), max_threads(max_threads_), handler(handler_)
    {
    }

    ParallelInputsProcessor(const BlockInputStreamPtr & mt_stream_, unsigned max_threads_, Handler & handler_)
        : input(mt_stream_), max_threads(max_threads_), handler(handler_)
    {
    }

    ~ParallelInputsProcessor()
    {
        try
        {
            wait();
        }
        catch (...)
        {
        }
    }

    /// Start background threads, start work.
    void process()
    {
        active_threads = max_threads;
        threads.reserve(max_threads);

        try
        {
            for (unsigned i = 0; i < max_threads; ++i)
                threads.emplace_back(&ParallelInputsProcessor::thread, this, i);
        }
        catch (...)
        {
            cancel(false);
            wait();
            if (active_threads)
            {
                active_threads = 0;
                /// handler.onFinish() is supposed to be called from one of the threads when the number of
                /// finished threads reaches max_threads. But since we weren't able to launch all threads,
                /// we have to call onFinish() manually here.
                handler.onFinish();
            }
            throw;
        }
    }

    void wait()
    {
        if (joined_threads)
            return;

        for (auto & thread : threads)
            thread.join();

        threads.clear();
        joined_threads = true;
    }

    void cancel(bool kill) { input->cancel(kill); }
    size_t getNumActiveThreads() const { return active_threads; }

private:
    void thread(unsigned thread_num)
    {
        std::exception_ptr exception;

        try
        {
            while (!input->isCancelled())
            {
                if (Block block = input->read())
                {
                    if (input->isCancelled())
                        break;
                    handler.onBlock(std::move(block), thread_num);
                }
                else
                    break;
            }
            handler.onFinishThread(thread_num);
        }
        catch (...)
        {
            exception = std::current_exception();
        }

        if (exception)
            handler.onException(exception, thread_num);

        /// The last thread on the output indicates that there is no more data.
        if (0 == --active_threads)
        {
            try
            {
                handler.onFinish();
            }
            catch (...)
            {
                exception = std::current_exception();
            }

            if (exception)
                handler.onException(exception, thread_num);
        }
    }

    BlockInputStreamPtr input;
    const unsigned max_threads;
    Handler & handler;

    std::vector<std::thread> threads;
    std::atomic<size_t> active_threads{0};
    std::atomic<bool> joined_threads{false};
};

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

    static void
    copyData(const BlockInputStreams & inputs, BlockOutputStreamPtr output, uint32_t max_threads, ProgressCallback progress = {})
    {
        BlockOutputStreamPtr mt_output = std::make_shared<GuardedBlockOutputStream>(output);
        mt_output->writePrefix();

        constexpr uint32_t flags = ParallelInputsStream::PREFIX | ParallelInputsStream::SUFFIX;
        ParallelInputsSink sink(inputs, mt_output, max_threads, flags, progress);
        sink.process();
        sink.finalize();

        mt_output->writeSuffix();
    }

    ParallelInputsSink(
        const BlockInputStreams & inputs,
        BlockOutputStreamPtr mt_output,
        uint32_t max_threads,
        uint32_t flags = 0,
        ProgressCallback progress = {})
        : handler(*this, progress), output(mt_output), processor(inputs, max_threads, handler, flags)
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
