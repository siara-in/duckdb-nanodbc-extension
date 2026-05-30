#define DUCKDB_EXTENSION_MAIN

#include "nanodbc_extension.hpp"
#include "duckdb.hpp"
#include "odbc_scanner.hpp"

namespace duckdb {

static void RegisterOdbcFunctions(ExtensionLoader &loader) {
    // Register each function separately to avoid copy issues
    loader.RegisterFunction(OdbcScanFunction());
    loader.RegisterFunction(OdbcAttachFunction());
    loader.RegisterFunction(OdbcQueryFunction());
    loader.RegisterFunction(OdbcExecFunction());
    loader.RegisterFunction(OdbcConnectFunction());
}

static void LoadInternal(ExtensionLoader &loader) {
    // Register the ODBC functions
    RegisterOdbcFunctions(loader);

}

void NanodbcExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void nanodbc_init(duckdb::DatabaseInstance &db) {
    static duckdb::NanodbcExtension ext;
    duckdb::ExtensionLoader loader(db, ext.Name());
    ext.Load(loader);
}
DUCKDB_EXTENSION_API void nanodbc_duckdb_cpp_init(duckdb::DatabaseInstance &db) {
    static duckdb::NanodbcExtension ext;
    duckdb::ExtensionLoader loader(db, ext.Name());
    ext.Load(loader);
}
DUCKDB_EXTENSION_API const char *nanodbc_version() {
    return nanodbc_ver;
}
}
