#include "Exception.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <cxxabi.h>
#include <sys/statvfs.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <common/demangle.h>
#include <Common/MemorySanitizer.h>

namespace fs = std::filesystem;

namespace AH
{

class SensitiveDataMaskerDumb
{
public:
    SensitiveDataMaskerDumb() = default;

    size_t wipeSensitiveData(std::string & data) const { return 0; }
    static SensitiveDataMaskerDumb * getInstance()
    {
        static SensitiveDataMaskerDumb dumb;
        return &dumb;
    }
    size_t rulesCount() const { return 0; }
};


void abortOnFailedAssertion(const String & description)
{
    //LOG_FATAL(&Poco::Logger::root(), "Logical error: '{}'.", description);
    abort();
}

Exception::MessageMasked::MessageMasked(const std::string & msg_) : msg(msg_)
{
    if (auto masker = SensitiveDataMaskerDumb::getInstance())
        masker->wipeSensitiveData(msg);
}

Exception::MessageMasked::MessageMasked(std::string && msg_) : msg(std::move(msg_))
{
    if (auto masker = SensitiveDataMaskerDumb::getInstance())
        masker->wipeSensitiveData(msg);
}

Exception::Exception(const MessageMasked & msg_masked) : msg_(msg_masked.msg)
{
    capture_thread_frame_pointers = thread_frame_pointers;
}

Exception::Exception(MessageMasked && msg_masked) : msg_(msg_masked.msg)
{
    capture_thread_frame_pointers = thread_frame_pointers;
}

Exception::Exception(CreateFromSTDTag, const std::exception & exc)
    : msg_(demangle(typeid(exc).name()) + ": " + String(exc.what()))
{
    capture_thread_frame_pointers = thread_frame_pointers;
#ifdef STD_EXCEPTION_HAS_STACK_TRACE
    auto * stack_trace_frames = exc.get_stack_trace_frames();
    auto stack_trace_size = exc.get_stack_trace_size();
    __msan_unpoison(stack_trace_frames, stack_trace_size * sizeof(stack_trace_frames[0]));
    set_stack_trace(stack_trace_frames, stack_trace_size);
#endif
}


std::string getExceptionStackTraceString(const std::exception & e)
{
#ifdef STD_EXCEPTION_HAS_STACK_TRACE
    auto * stack_trace_frames = e.get_stack_trace_frames();
    auto stack_trace_size = e.get_stack_trace_size();
    __msan_unpoison(stack_trace_frames, stack_trace_size * sizeof(stack_trace_frames[0]));
    return StackTrace::toString(stack_trace_frames, 0, stack_trace_size);
#else
    if (const auto * db_exception = dynamic_cast<const Exception *>(&e))
        return db_exception->getStackTraceString();
    return {};
#endif
}

std::string getExceptionStackTraceString(std::exception_ptr e)
{
    try
    {
        std::rethrow_exception(e);
    }
    catch (const std::exception & exception)
    {
        return getExceptionStackTraceString(exception);
    }
    catch (...)
    {
        return {};
    }
}


std::string Exception::getStackTraceString() const
{
#ifdef STD_EXCEPTION_HAS_STACK_TRACE
    auto * stack_trace_frames = get_stack_trace_frames();
    auto stack_trace_size = get_stack_trace_size();
    __msan_unpoison(stack_trace_frames, stack_trace_size * sizeof(stack_trace_frames[0]));
    String thread_stack_trace;
    std::for_each(
        capture_thread_frame_pointers.rbegin(),
        capture_thread_frame_pointers.rend(),
        [&thread_stack_trace](StackTrace::FramePointers & frame_pointers)
        {
            thread_stack_trace += "\nJob's origin stack trace:\n"
                + StackTrace::toString(frame_pointers.data(), 0, std::ranges::find(frame_pointers, nullptr) - frame_pointers.begin());
        });

    return StackTrace::toString(stack_trace_frames, 0, stack_trace_size) + thread_stack_trace;
#else
    return trace.toString();
#endif
}

Exception::FramePointers Exception::getStackFramePointers() const
{
    FramePointers frame_pointers;
#ifdef STD_EXCEPTION_HAS_STACK_TRACE
    {
        frame_pointers.resize(get_stack_trace_size());
        for (size_t i = 0; i < frame_pointers.size(); ++i)
            frame_pointers[i] = get_stack_trace_frames()[i];
        __msan_unpoison(frame_pointers.data(), frame_pointers.size() * sizeof(frame_pointers[0]));
    }
#else
    {
        size_t stack_trace_size = trace.getSize();
        size_t stack_trace_offset = trace.getOffset();
        frame_pointers.reserve(stack_trace_size - stack_trace_offset);
        for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
            frame_pointers.push_back(trace.getFramePointers()[i]);
    }
#endif
    return frame_pointers;
}

thread_local std::vector<StackTrace::FramePointers> Exception::thread_frame_pointers = {};


void tryLogCurrentException(const char *, const std::string &)
{
    // TODO
}

static struct statvfs getStatVFS(String path)
{
    struct statvfs fs;
    while (statvfs(path.c_str(), &fs) != 0)
    {
        if (errno == EINTR)
            continue;

        /// Sometimes we create directories lazily, so we can request free space in a directory that yet to be created.
        auto fs_path = std::filesystem::path(path);
        if (errno == ENOENT && fs_path.has_parent_path())
        {
            path = fs_path.parent_path();
            continue;
        }

        throw ErrnoException("Could not calculate available disk space (statvfs)", errno);
    }
    return fs;
}

static void getNoSpaceLeftInfoMessage(std::filesystem::path path, String & msg)
{
    path = std::filesystem::absolute(path);
    /// It's possible to get ENOSPC for non existent file (e.g. if there are no free inodes and creat() fails)
    /// So try to get info for existent parent directory.
    while (!std::filesystem::exists(path) && path.has_relative_path())
        path = path.parent_path();

    auto fs = getStatVFS(path);
#if 0
    auto mount_point = getMountPoint(path).string();
    fmt::format_to(
        std::back_inserter(msg),
        "\nTotal space: {}\nAvailable space: {}\nTotal inodes: {}\nAvailable inodes: {}\nMount point: {}",
        ReadableSize(fs.f_blocks * fs.f_frsize),
        ReadableSize(fs.f_bavail * fs.f_frsize),
        formatReadableQuantity(fs.f_files),
        formatReadableQuantity(fs.f_favail),
        mount_point);
#else
    fmt::format_to(std::back_inserter(msg), "\nTotal space: {}\nAvailable space: {}", fs.f_blocks * fs.f_frsize, fs.f_bavail * fs.f_frsize);
#endif

#if defined(OS_LINUX)
    msg += "\nFilesystem: " + getFilesystemName(mount_point);
#endif
}


/** It is possible that the system has enough memory,
  *  but we have shortage of the number of available memory mappings.
  * Provide good diagnostic to user in that case.
  */
static void getNotEnoughMemoryMessage(std::string & msg)
{
#if defined(OS_LINUX)
    try
    {
        static constexpr size_t buf_size = 1024;
        char buf[buf_size];

        UInt64 max_map_count = 0;
        {
            ReadBufferFromFile file("/proc/sys/vm/max_map_count", buf_size, -1, buf);
            readText(max_map_count, file);
        }

        UInt64 num_maps = 0;
        {
            ReadBufferFromFile file("/proc/self/maps", buf_size, -1, buf);
            while (!file.eof())
            {
                char * next_pos = find_first_symbols<'\n'>(file.position(), file.buffer().end());
                file.position() = next_pos;

                if (!file.hasPendingData())
                    continue;

                if (*file.position() == '\n')
                {
                    ++num_maps;
                    ++file.position();
                }
            }
        }

        if (num_maps > max_map_count * 0.90)
        {
            msg += fmt::format(
                "\nIt looks like that the process is near the limit on number of virtual memory mappings."
                "\nCurrent number of mappings (/proc/self/maps): {}."
                "\nLimit on number of mappings (/proc/sys/vm/max_map_count): {}."
                "\nYou should increase the limit for vm.max_map_count in /etc/sysctl.conf"
                "\n",
                num_maps,
                max_map_count);
        }
    }
    catch (...)
    {
        msg += "\nCannot obtain additional info about memory usage.";
    }
#else
    (void)msg;
#endif
}

std::string getExtraExceptionInfo(const std::exception & e)
{
    String msg;
    try
    {
        if (const auto * file_exception = dynamic_cast<const fs::filesystem_error *>(&e))
        {
            if (file_exception->code() == std::errc::no_space_on_device)
                getNoSpaceLeftInfoMessage(file_exception->path1(), msg);
            else
                msg += "\nCannot print extra info for Exception";
        }
        else if (const auto * errno_exception = dynamic_cast<const ErrnoException *>(&e))
        {
            if (errno_exception->getErrno() == ENOSPC && errno_exception->getPath())
                getNoSpaceLeftInfoMessage(errno_exception->getPath().value(), msg);
        }
        else if (dynamic_cast<const std::bad_alloc *>(&e))
        {
            getNotEnoughMemoryMessage(msg);
        }
    }
    catch (...)
    {
        msg += "\nCannot print extra info: " + getCurrentExceptionMessage(false, false, false);
    }

    return msg;
}

std::string getCurrentExceptionMessage(bool with_stacktrace, bool check_embedded_stacktrace /*= false*/, bool with_extra_info /*= true*/)
{
    WriteBufferFromOwnString stream;

    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        stream << getExceptionMessage(e, with_stacktrace, check_embedded_stacktrace) << (with_extra_info ? getExtraExceptionInfo(e) : "");
    }
    catch (const std::exception & e)
    {
        try
        {
            int status = 0;
            auto name = demangle(typeid(e).name(), status);

            if (status)
                name += " (demangling status: " + std::to_string(status) + ")";

            stream << "std::exception, type: " << name << ", e.what() = " << e.what()
                   << (with_stacktrace ? ", Stack trace (when copying this message, always include the lines below):\n\n"
                               + getExceptionStackTraceString(e)
                                       : "")
                   << (with_extra_info ? getExtraExceptionInfo(e) : "");
        }
        catch (...)
        {
        } // NOLINT(bugprone-empty-catch)

#ifdef ABORT_ON_LOGICAL_ERROR
        try
        {
            throw;
        }
        catch (const std::logic_error &)
        {
            if (!with_stacktrace)
                stream << ", Stack trace:\n\n" << getExceptionStackTraceString(e);

            abortOnFailedAssertion(stream.str());
        }
        catch (...)
        {
        } // NOLINT(bugprone-empty-catch)
#endif
    }
    catch (...)
    {
        try
        {
            int status = 0;
            auto name = demangle(abi::__cxa_current_exception_type()->name(), status);

            if (status)
                name += " (demangling status: " + std::to_string(status) + ")";

            stream << "Unknown exception, type: " << name;
        }
        catch (...)
        {
        } // NOLINT(bugprone-empty-catch)
    }

    return stream.str();
}


void tryLogException(std::exception_ptr e, const char * log_name, const std::string & start_of_message)
{
    try
    {
        std::rethrow_exception(std::move(e)); // NOLINT
    }
    catch (...)
    {
        tryLogCurrentException(log_name, start_of_message);
    }
}

std::string getExceptionMessage(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace)
{
    WriteBufferFromOwnString stream;

    try
    {
        std::string text = e.displayText();

        bool has_embedded_stack_trace = false;
        if (check_embedded_stacktrace)
        {
            auto embedded_stack_trace_pos = text.find("Stack trace");
            has_embedded_stack_trace = embedded_stack_trace_pos != std::string::npos;
            if (!with_stacktrace && has_embedded_stack_trace)
            {
                text.resize(embedded_stack_trace_pos);
                //Poco::trimRightInPlace(text);
            }
        }

        stream << text;

        if (!text.empty() && text.back() != '.')
            stream << '.';

        if (with_stacktrace && !has_embedded_stack_trace)
            stream << " Stack trace (when copying this message, always include the lines below):\n\n" << e.getStackTraceString();
    }
    catch (...)
    {
    } // NOLINT(bugprone-empty-catch)

    return stream.str();
}

std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace)
{
    try
    {
        std::rethrow_exception(std::move(e)); // NOLINT
    }
    catch (...)
    {
        return getCurrentExceptionMessage(with_stacktrace);
    }
}


std::string ExecutionStatus::serializeText() const
{
    WriteBufferFromOwnString wb;
    wb << message;
    return wb.str();
}

void ExecutionStatus::deserializeText(const std::string & data)
{
    ReadBufferFromString rb(data);
    rb >> message;
}

bool ExecutionStatus::tryDeserializeText(const std::string & data)
{
    try
    {
        deserializeText(data);
    }
    catch (...)
    {
        return false;
    }

    return true;
}

ExecutionStatus ExecutionStatus::fromCurrentException(const std::string & start_of_message, bool with_stacktrace)
{
    String msg = (start_of_message.empty() ? "" : (start_of_message + ": ")) + getCurrentExceptionMessage(with_stacktrace, true);
    return ExecutionStatus(msg);
}

ExecutionStatus ExecutionStatus::fromText(const std::string & data)
{
    ExecutionStatus status;
    status.deserializeText(data);
    return status;
}
}
