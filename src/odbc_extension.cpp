#define DUCKDB_EXTENSION_MAIN

#include "odbc_extension.hpp"
#include "duckdb.hpp"
#include "odbc_db.hpp"
#include "odbc_scanner.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

static void SetODBCDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
    ODBCDB::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

static void LoadInternal(DatabaseInstance &instance) {
    // Debug print during loading
    fprintf(stderr, "ODBC Extension: Loading functions\n");
    
    // Register the ODBC scan function
    ODBCScanFunction odbc_fun;
    ExtensionUtil::RegisterFunction(instance, odbc_fun);
    fprintf(stderr, "ODBC Extension: Registered scan function\n");

    // Register the ODBC attach function
    ODBCAttachFunction attach_func;
    ExtensionUtil::RegisterFunction(instance, attach_func);
    fprintf(stderr, "ODBC Extension: Registered attach function\n");

    // Register the ODBC query function
    ODBCQueryFunction query_func;
    ExtensionUtil::RegisterFunction(instance, query_func);
    fprintf(stderr, "ODBC Extension: Registered query function\n");

    // Add extension options
    auto &config = DBConfig::GetConfig(instance);
    config.AddExtensionOption("odbc_all_varchar", "Load all ODBC columns as VARCHAR columns", LogicalType::BOOLEAN);
    
    config.AddExtensionOption("odbc_debug_show_queries", "DEBUG SETTING: print all queries sent to ODBC to stdout",
                              LogicalType::BOOLEAN, Value::BOOLEAN(false), SetODBCDebugQueryPrint);
                              
    fprintf(stderr, "ODBC Extension: Successfully loaded\n");
}

void OdbcExtension::Load(DuckDB &db) {
    fprintf(stderr, "ODBC Extension::Load called\n");
    LoadInternal(*db.instance);
}

std::string OdbcExtension::Name() {
    return "odbc";
}

std::string OdbcExtension::Version() const {
#ifdef EXT_VERSION_ODBC
    return EXT_VERSION_ODBC;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void odbc_init(duckdb::DatabaseInstance &db) {
    fprintf(stderr, "ODBC Extension: odbc_init called\n");
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::OdbcExtension>();
}

DUCKDB_EXTENSION_API const char *odbc_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif