#pragma once

#include "duckdb.hpp"
#include "odbc_headers.hpp"
#include <memory>

namespace duckdb {

// Forward declarations
class OdbcStatement;

// Connection parameters structure
struct ConnectionParams {
    std::string Dsn;
    std::string ConnectionString;
    std::string Username;
    std::string Password;
    int Timeout = 60;
    bool ReadOnly = true;

    // Helper to check if we have valid connection information
    bool IsValid() const {
        return !Dsn.empty() || !ConnectionString.empty();
    }
    
    // Create from DSN
    static ConnectionParams FromDsn(const std::string& dsn, 
                                  const std::string& username = "",
                                  const std::string& password = "",
                                  int timeout = 60,
                                  bool readOnly = true) {
        ConnectionParams params;
        params.Dsn = dsn;
        params.Username = username;
        params.Password = password;
        params.Timeout = timeout;
        params.ReadOnly = readOnly;
        return params;
    }

    // Create from connection string
    static ConnectionParams FromConnectionString(const std::string& connectionString,
                                               int timeout = 60,
                                               bool readOnly = true) {
        ConnectionParams params;
        params.ConnectionString = connectionString;
        params.Timeout = timeout;
        params.ReadOnly = readOnly;
        return params;
    }
};

class OdbcConnection {
public:
    OdbcConnection();
    ~OdbcConnection();

    // Move semantics
    OdbcConnection(OdbcConnection &&other) noexcept;
    OdbcConnection &operator=(OdbcConnection &&other) noexcept;

    // Forbid copying
    OdbcConnection(const OdbcConnection &) = delete;
    OdbcConnection &operator=(const OdbcConnection &) = delete;

    // Connect to ODBC data source
    static OdbcConnection Connect(const ConnectionParams& params);

    // Prepare a statement
    OdbcStatement Prepare(const std::string &query);

    // Execute a simple statement (no results)
    void Execute(const std::string &query);

    // Check if the connection is open
    bool IsOpen() const;

    // Close the connection
    void Close();

    // Get tables from the connection
    std::vector<std::string> GetTables();

    // Get columns for a table
    void GetTableInfo(const std::string &tableName, ColumnList &columns, 
                     std::vector<std::unique_ptr<Constraint>> &constraints, bool allVarchar = false);

    // Checks if a column exists in the specified table
    bool ColumnExists(const std::string &tableName, const std::string &columnName);

    // Get catalog entry type
    CatalogType GetEntryType(const std::string &name);

    // Debug helper
    static void SetDebugPrintQueries(bool print);

    // Get raw nanodbc connection (for advanced usage)
    nanodbc::connection& GetNativeConnection() { return connection; }
    const nanodbc::connection& GetNativeConnection() const { return connection; }

private:
    nanodbc::connection connection;
    bool owner;
    static bool debugPrintQueries;
};

} // namespace duckdb