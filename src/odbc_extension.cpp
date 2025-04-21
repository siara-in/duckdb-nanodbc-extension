#define DUCKDB_EXTENSION_MAIN

#include "odbc_extension.hpp"
#include "duckdb.hpp"
#include "odbc_connection.hpp"
#include "odbc_scanner.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

static void SetOdbcDebugPrintQueries(ClientContext &context, SetScope scope, Value &parameter) {
    OdbcConnection::SetDebugPrintQueries(BooleanValue::Get(parameter));
}

static void RegisterOdbcFunctions(DatabaseInstance &instance) {
    // Register all ODBC functions
    std::vector<TableFunction> functions = {
        OdbcScanFunction(),
        OdbcAttachFunction(),
        OdbcQueryFunction()
    };
    
    for (auto& func : functions) {
        ExtensionUtil::RegisterFunction(instance, func);
    }
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register the ODBC functions
    RegisterOdbcFunctions(instance);
    
    // Add extension options
    auto &config = DBConfig::GetConfig(instance);
    
    config.AddExtensionOption("odbc_all_varchar", 
                            "Load all ODBC columns as VARCHAR columns", 
                            LogicalType(LogicalTypeId::BOOLEAN));
    
    config.AddExtensionOption("odbc_debug_show_queries", 
                            "DEBUG SETTING: print all queries sent to ODBC to stdout",
                            LogicalType(LogicalTypeId::BOOLEAN), 
                            false, 
                            SetOdbcDebugPrintQueries);
}

void OdbcExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

} // namespace duckdb

extern "C" {

// Critical: Make sure these function names exactly match what DuckDB expects
DUCKDB_EXTENSION_API void odbc_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::OdbcExtension>();
}

DUCKDB_EXTENSION_API const char *odbc_extension_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}