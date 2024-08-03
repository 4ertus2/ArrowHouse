#pragma once

//#include <Core/Settings.h>
#include <memory>
#include <string>
#include <vector>


namespace AH
{

class Context;

/// Lightweight Application for clickhouse-local
/// No networking, no extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
class LocalServer
{
public:
    LocalServer();
    ~LocalServer();

    //void initialize(Poco::Util::Application & self);

    int main(const std::vector<std::string> & args);
    void init(int argc, char ** argv);

private:
    /** Composes CREATE subquery based on passed arguments (--structure --file --table and --input-format)
      * This query will be executed first, before queries passed through --query argument
      * Returns empty string if it cannot compose that query.
      */
    std::string getInitialCreateTableQuery();

    //void tryInitPath();
    void applyCmdOptions();
    void applyCmdSettings();
    void attachSystemTables();
    void processQueries();
    void setupUsers();

    std::string getHelpHeader() const;
    std::string getHelpFooter() const;

protected:
    std::unique_ptr<Context> context;

    /// Settings specified via command line args
    //Settings cmd_settings;
};

}
