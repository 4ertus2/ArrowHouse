#pragma once

#include <atomic>
#include <exception>
#include <mutex>
#include <queue>
#include <semaphore>
#include <thread>
#include <unordered_map>
#include <vector>

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


struct NoSemaphore
{
    NoSemaphore(std::ptrdiff_t /*desired*/) { }
    void acquire() { }
    void release(std::ptrdiff_t update = 1) { }
};


template <typename TSemaphore>
class SemaphoreGuard
{
public:
    SemaphoreGuard(TSemaphore & sema_) : sema(sema_) { sema.acquire(); }
    ~SemaphoreGuard() { sema.release(); }

private:
    TSemaphore & sema;
};


struct ParallelInput
{
    enum Flags
    {
        NONE = 0,
        PREFIX = 0x1,
        SUFFIX = 0x2,
        AFFINITY = 0x4,
    };

    BlockInputStreamPtr stream;
    uint32_t flags = NONE;
    size_t i = 0;

    ParallelInput(const BlockInputStreamPtr & in = {}, uint32_t flags_ = 0, size_t i_ = 0) : stream(in), flags(flags_), i(i_) { }

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


class AvailableInputsQueue
{
public:
    using Input = ParallelInput;

    AvailableInputsQueue(const BlockInputStreams & inputs_, uint32_t flags)
    {
        for (size_t i = 0; i < inputs_.size(); ++i)
            inputs.emplace(inputs_[i], flags, i);
    }

    Input popInput()
    {
        std::lock_guard lock(mutex);

        if (inputs.empty())
            return {};

        Input input = inputs.front();
        inputs.pop();
        return input;
    }

    void pushInput(Input && input)
    {
        std::lock_guard lock(mutex);

        inputs.emplace(std::move(input));
    }

private:
    std::queue<Input> inputs;
    std::mutex mutex;
};


class AffinedInputs
{
public:
    using Input = ParallelInput;

    AffinedInputs(const BlockInputStreams & inputs_, uint32_t flags)
    {
        inputs.reserve(inputs_.size());
        for (size_t i = 0; i < inputs_.size(); ++i)
            inputs.push_back(ParallelInput(inputs_[i], flags, i));
    }

    Input popInput()
    {
        auto thread_id = std::this_thread::get_id();
        std::lock_guard lock(mutex);

        auto it = affinity.find(thread_id);
        if (it != affinity.end())
        {
            return std::move(inputs[it->second]);
        }
        else if (affinity.size() < inputs.size())
        {
            size_t input_pos = affinity.size();
            affinity.emplace(thread_id, input_pos);
            return std::move(inputs[input_pos]);
        }
        return {};
    }

    void pushInput(Input && input)
    {
        auto thread_id = std::this_thread::get_id();
        std::lock_guard lock(mutex);

        inputs[affinity[thread_id]] = std::move(input);
    }

private:
    std::vector<Input> inputs;
    std::unordered_map<std::thread::id, size_t> affinity;
    std::mutex mutex;
};


template <typename TSemapore = NoSemaphore, typename TQueue = AvailableInputsQueue>
class TParallelInputsStream : public IBlockInputStream
{
public:
    static constexpr ptrdiff_t MAX_SEMA_1K = 1000;

    TParallelInputsStream(const BlockInputStreams & inputs_, uint32_t max_io_threads, uint32_t flags)
        : queue(inputs_, flags), io_semaphore(max_io_threads)
    {
        if (inputs_.empty())
            throw std::runtime_error("unexpected empty inputs for ParallelInputsStream");

        children = inputs_;
    }

    String getName() const override { return "ParallelInputsStream"; }
    Header getHeader() const override { return children[0]->getHeader(); }

    Block readImpl() override
    {
        {
            SemaphoreGuard<TSemapore> guard(io_semaphore);

            ParallelInput input = queue.popInput();
            if (!input.stream)
                return {};

            if (Block block = input.read())
            {
                queue.pushInput(std::move(input));
                return block;
            }
        }

        if (isCancelled())
            return {};
        return read();
    }

private:
    TQueue queue;
    TSemapore io_semaphore;
};

using ParallelInputsStream = TParallelInputsStream<>;
using IOLimitedInputsStream = TParallelInputsStream<std::counting_semaphore<ParallelInputsStream::MAX_SEMA_1K>>;
using AffinedInputsStream = TParallelInputsStream<NoSemaphore, AffinedInputs>;


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
    static BlockInputStreamPtr
    MakeInputsStream(const BlockInputStreams & inputs_, unsigned max_compute_threads, unsigned max_io_threads, uint32_t flags = 0)
    {
        if (flags & ParallelInput::AFFINITY)
            return std::make_shared<AffinedInputsStream>(inputs_, inputs_.size(), flags);

        if (max_io_threads && (max_compute_threads > max_io_threads))
            return std::make_shared<IOLimitedInputsStream>(inputs_, max_io_threads, flags);

        return std::make_shared<ParallelInputsStream>(inputs_, max_io_threads, flags);
    }

    ParallelInputsProcessor(
        const BlockInputStreams & inputs_, unsigned max_compute_threads, unsigned max_io_threads, Handler & handler_, uint32_t flags = 0)
        : input(MakeInputsStream(inputs_, max_compute_threads, max_io_threads, flags)), max_threads(max_compute_threads), handler(handler_)
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

}
