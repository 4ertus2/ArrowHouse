#pragma once

#include <atomic>
#include <exception>
#include <mutex>
#include <queue>
#include <thread>

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

class ParallelInputStream : public IBlockInputStream
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

    ParallelInputStream(const BlockInputStreams & inputs_, uint32_t flags = NONE)
    {
        if (inputs_.empty())
            throw std::runtime_error("unexpected empty inputs for ParallelInputStream");

        children = inputs_;

        for (size_t i = 0; i < inputs_.size(); ++i)
            available_inputs.emplace(inputs_[i], i);
    }

    String getName() const override { return "ParallelInputStream"; }
    Header getHeader() const override { return children[0]->getHeader(); }

    Block readImpl() override
    {
        Input input = popInput();
        if (input.stream)
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
    void onFinishThread(size_t /*thread_num*/) { }

    /// Blocks are over. Due to the fact that all sources ran out or because of the cancellation of work.
    /// This method is always called exactly once, at the end of the work, if the `onException` method does not throw an exception.
    void onFinish() { }

    /// Exception handling. It is reasonable to call the ParallelInputsProcessor::cancel method in this method, and also pass the exception to the main thread.
    void onException(std::exception_ptr & /*exception*/, size_t /*thread_num*/) { }
};


template <typename Handler>
class ParallelInputsProcessor
{
public:
    ParallelInputsProcessor(const BlockInputStreams & inputs_, unsigned max_threads_, Handler & handler_)
        : input(std::make_shared<ParallelInputStream>(inputs_)), max_threads(max_threads_), handler(handler_)
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
            for (size_t i = 0; i < max_threads; ++i)
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
    void thread(size_t thread_num)
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

}
