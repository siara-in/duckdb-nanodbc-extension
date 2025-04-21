#pragma once

#include "duckdb.hpp"
#include "odbc_connection.hpp"
#include "odbc_statement.hpp"
#include "odbc_headers.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

// Make ScannerState inherit from TableFunctionData
struct ScannerState : public TableFunctionData {
    std::string ConnectionString;
    std::string Dsn;
    std::string Username;
    std::string Password;
    std::string TableName;
    std::string Sql;
    std::vector<std::string> Names;
    std::vector<LogicalType> Types;
    bool AllVarchar = false;
    OdbcConnection* GlobalConnection = nullptr;
    
    // Store named parameters directly instead of using Table pointer
    std::map<std::string, Value> named_parameters;
    
    // Helper method to create ConnectionParams from this state
    ConnectionParams CreateConnectionParams() const {
        if (!Dsn.empty()) {
            return ConnectionParams::FromDsn(Dsn, Username, Password);
        } else {
            return ConnectionParams::FromConnectionString(ConnectionString);
        }
    }
};
struct LocalScanState : public LocalTableFunctionState {
    OdbcConnection* Connection;
    OdbcConnection OwnedConnection;
    OdbcStatement Statement;
    bool Done = false;
    std::vector<column_t> ColumnIds;
    idx_t ScanCount = 0;
};

struct GlobalScanState : public GlobalTableFunctionState {
    explicit GlobalScanState(idx_t maxThreads) : maxThreadCount(maxThreads) {}

    std::mutex Lock;
    idx_t Position = 0;
    idx_t maxThreadCount;

    idx_t MaxThreads() const override {
        return maxThreadCount;
    }
};

// Table function declarations
class OdbcScanFunction : public TableFunction {
public:
    OdbcScanFunction();
};

class OdbcAttachFunction : public TableFunction {
public:
    OdbcAttachFunction();
};

class OdbcQueryFunction : public TableFunction {
public:
    OdbcQueryFunction();
};

// Helper functions for scanning
void ScanOdbcTable(ClientContext &context, TableFunctionInput &data, DataChunk &output);
unique_ptr<GlobalTableFunctionState> InitGlobalScanState(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<LocalTableFunctionState> InitLocalScanState(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state);

// Binding functions for different operations
unique_ptr<FunctionData> BindScan(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
unique_ptr<FunctionData> BindAttach(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
unique_ptr<FunctionData> BindQuery(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);

// Implementation function for attach
void AttachDatabase(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb