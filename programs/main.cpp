#include <iostream>
#include <new>
#include <string>
#include <utility> /// pair
#include <vector>

//#if __has_include("config_tools.h")
//#include "config_tools.h"
//#endif
//#if __has_include("config_core.h")
//#include "config_core.h"
//#endif

#include <Common/StringUtils.h>

//#include <common/phdr_cache.h>
#include <common/scope_guard.h>


/// Universal executable for various clickhouse applications
#if ENABLE_CLICKHOUSE_LOCAL || !defined(ENABLE_CLICKHOUSE_LOCAL)
int mainEntryClickHouseLocal(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_BENCHMARK || !defined(ENABLE_CLICKHOUSE_BENCHMARK)
int mainEntryClickHouseBenchmark(int argc, char ** argv);
#endif


namespace
{

using MainFunc = int (*)(int, char **);


/// Add an item here to register new application
std::pair<const char *, MainFunc> clickhouse_applications[] = {
#if ENABLE_CLICKHOUSE_LOCAL || !defined(ENABLE_CLICKHOUSE_LOCAL)
    {"local", mainEntryClickHouseLocal},
#endif
#if ENABLE_CLICKHOUSE_BENCHMARK || !defined(ENABLE_CLICKHOUSE_BENCHMARK)
    {"benchmark", mainEntryClickHouseBenchmark},
#endif
};


int printHelp(int, char **)
{
    std::cerr << "Use one of the following commands:" << std::endl;
    for (auto & application : clickhouse_applications)
        std::cerr << "clickhouse " << application.first << " [args] " << std::endl;
    return -1;
}


bool isClickhouseApp(const std::string & app_suffix, std::vector<char *> & argv)
{
    /// Use app if the first arg 'app' is passed (the arg should be quietly removed)
    if (argv.size() >= 2)
    {
        auto first_arg = argv.begin() + 1;

        /// 'clickhouse --client ...' and 'clickhouse client ...' are Ok
        if (*first_arg == "--" + app_suffix || *first_arg == app_suffix)
        {
            argv.erase(first_arg);
            return true;
        }
    }

    /// Use app if clickhouse binary is run through symbolic link with name clickhouse-app
    std::string app_name = "clickhouse-" + app_suffix;
    return !argv.empty() && (app_name == argv[0] || endsWith(argv[0], "/" + app_name));
}

}


/// This allows to implement assert to forbid initialization of a class in static constructors.
/// Usage:
///
/// extern bool inside_main;
/// class C { C() { assert(inside_main); } };
bool inside_main = false;


int main(int argc_, char ** argv_)
{
    inside_main = true;
    SCOPE_EXIT({ inside_main = false; });

    /// Reset new handler to default (that throws std::bad_alloc)
    /// It is needed because LLVM library clobbers it.
    std::set_new_handler(nullptr);

    /// PHDR cache is required for query profiler to work reliably
    /// It also speed up exception handling, but exceptions from dynamically loaded libraries (dlopen)
    ///  will work only after additional call of this function.
    //updatePHDRCache();

    std::vector<char *> argv(argv_, argv_ + argc_);

    /// Print a basic help if nothing was matched
    MainFunc main_func = printHelp;

    for (auto & application : clickhouse_applications)
    {
        if (isClickhouseApp(application.first, argv))
        {
            main_func = application.second;
            break;
        }
    }

    return main_func(static_cast<int>(argv.size()), argv.data());
}
