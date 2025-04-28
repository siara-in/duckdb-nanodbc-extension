#define DUCKDB_EXTENSION_MAIN

#include "nanodbc_extension.hpp"
#include "duckdb.hpp"
#include "odbc_scanner.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

static void RegisterOdbcFunctions(DatabaseInstance &instance) {
    // Register each function separately to avoid copy issues
    ExtensionUtil::RegisterFunction(instance, OdbcScanFunction());
    ExtensionUtil::RegisterFunction(instance, OdbcAttachFunction());
    ExtensionUtil::RegisterFunction(instance, OdbcQueryFunction());
    ExtensionUtil::RegisterFunction(instance, OdbcExecFunction());
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register the ODBC functions
    RegisterOdbcFunctions(instance);

}

void NanodbcExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

} // namespace duckdb

extern "C" {

// Critical: Make sure these function names exactly match what DuckDB expects
DUCKDB_EXTENSION_API void nanodbc_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::NanodbcExtension>();
}

DUCKDB_EXTENSION_API const char *nanodbc_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif