#ifndef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_BUILD_LOADABLE_EXTENSION
#endif
#include "duckdb.hpp"

#include "odbc_db.hpp"
#include "odbc_scanner.hpp"
#include "odbc_extension.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

extern "C" {

static void SetODBCDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
    ODBCDB::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

static void LoadInternal(DatabaseInstance &db) {
    // Register the ODBC scan function
    ODBCScanFunction odbc_fun;
    ExtensionUtil::RegisterFunction(db, odbc_fun);

    // Register the ODBC attach function
    ODBCAttachFunction attach_func;
    ExtensionUtil::RegisterFunction(db, attach_func);

    // Register the ODBC query function
    ODBCQueryFunction query_func;
    ExtensionUtil::RegisterFunction(db, query_func);

    // Add extension options
    auto &config = DBConfig::GetConfig(db);
    config.AddExtensionOption("odbc_all_varchar", "Load all ODBC columns as VARCHAR columns", LogicalType::BOOLEAN);
    
    config.AddExtensionOption("odbc_debug_show_queries", "DEBUG SETTING: print all queries sent to ODBC to stdout",
                              LogicalType::BOOLEAN, Value::BOOLEAN(false), SetODBCDebugQueryPrint);
}

void ODBCScannerExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

DUCKDB_EXTENSION_API void odbc_scanner_init(duckdb::DatabaseInstance &db) {
    LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *odbc_scanner_version() {
    return DuckDB::LibraryVersion();
}

}