#include "nanodbc_db.hpp"
#include "nanodbc_stmt.hpp"
#include "nanodbc_utils.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

bool NanodbcDB::debug_print_queries = false;

NanodbcDB::NanodbcDB() : owner(false) {
}

NanodbcDB::~NanodbcDB() {
    Close();
}

NanodbcDB::NanodbcDB(NanodbcDB &&other) noexcept {
    conn = std::move(other.conn);
    owner = other.owner;
    other.owner = false;
}

NanodbcDB &NanodbcDB::operator=(NanodbcDB &&other) noexcept {
    if (this != &other) {
        Close();
        conn = std::move(other.conn);
        owner = other.owner;
        other.owner = false;
    }
    return *this;
}

NanodbcDB NanodbcDB::OpenWithDSN(const string &dsn, const string &username, const string &password, const ODBCOpenOptions &options) {
    try {
        NanodbcDB db;
        
        // Set connection timeout
        int timeout = std::stoi(options.connection_timeout);
        
        // Connect to ODBC data source
        if (username.empty() && password.empty()) {
            db.conn = nanodbc::connection(dsn, timeout);
        } else {
            db.conn = nanodbc::connection(dsn, username, password, timeout);
        }
        
        // Set read-only mode if supported
        if (options.read_only) {
            try {
                // Use native ODBC call for setting connection attributes that nanodbc doesn't expose
                SQLHDBC native_handle = db.conn.native_dbc_handle();
                SQLSetConnectAttr(native_handle, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);
            } catch (const std::exception& e) {
                // Just log warning, don't fail if read-only setting is not supported
                fprintf(stderr, "Warning: Failed to set read-only mode: %s\n", e.what());
            }
        }
        
        db.owner = true;
        return db;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to connect to DSN '" + dsn + "': " + NanodbcUtils::HandleException(e));
    }
}

NanodbcDB NanodbcDB::OpenWithConnectionString(const string &connection_string, const ODBCOpenOptions &options) {
    try {
        NanodbcDB db;
        
        // Set connection timeout
        int timeout = std::stoi(options.connection_timeout);
        
        // Connect using the connection string
        db.conn = nanodbc::connection(connection_string, timeout);
        
        // Set read-only mode if supported
        if (options.read_only) {
            try {
                // Use native ODBC call for setting connection attributes that nanodbc doesn't expose
                SQLHDBC native_handle = db.conn.native_dbc_handle();
                SQLSetConnectAttr(native_handle, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);
            } catch (const std::exception& e) {
                // Just log warning, don't fail if read-only setting is not supported
                fprintf(stderr, "Warning: Failed to set read-only mode: %s\n", e.what());
            }
        }
        
        db.owner = true;
        return db;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to connect with connection string: " + NanodbcUtils::HandleException(e));
    }
}

bool NanodbcDB::TryPrepare(const string &query, NanodbcStatement &stmt) {
    if (!IsOpen()) {
        return false;
    }
    
    if (debug_print_queries) {
        printf("ODBC Query: %s\n", query.c_str());
    }
    
    try {
        stmt = NanodbcStatement(conn, query);
        return true;
    } catch (const nanodbc::database_error&) {
        return false;
    }
}

NanodbcStatement NanodbcDB::Prepare(const string &query) {
    NanodbcStatement stmt;
    if (!TryPrepare(query, stmt)) {
        try {
            // Try again to get the actual error
            stmt = NanodbcStatement(conn, query);
        } catch (const nanodbc::database_error& e) {
            throw std::runtime_error("Failed to prepare query \"" + query + "\": " + NanodbcUtils::HandleException(e));
        }
    }
    return stmt;
}

void NanodbcDB::Execute(const string &query) {
    if (debug_print_queries) {
        printf("ODBC Query: %s\n", query.c_str());
    }
    
    try {
        nanodbc::just_execute(conn, query);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to execute query \"" + query + "\": " + NanodbcUtils::HandleException(e));
    }
}

bool NanodbcDB::IsOpen() const {
    return conn.connected();
}

void NanodbcDB::Close() {
    if (!owner) {
        return;
    }
    
    if (IsOpen()) {
        try {
            conn.disconnect();
        } catch (...) {
            // Ignore exceptions during disconnect
        }
    }
    
    owner = false;
}

std::vector<std::string> NanodbcDB::GetTables() {
    std::vector<std::string> tables;
    
    try {
        // Use nanodbc's catalog functions to get tables
        nanodbc::catalog catalog(conn);
        auto table_results = catalog.find_tables(std::string(), std::string(), std::string(), std::string("TABLE"));
        
        while (table_results.next()) {
            // Use the table_name() method instead of get<std::string>()
            std::string table_name = table_results.table_name();
            tables.push_back(table_name);
        }
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get table list: " + NanodbcUtils::HandleException(e));
    }
    
    return tables;
}

void NanodbcDB::GetTableInfo(const std::string &table_name, ColumnList &columns, 
                           std::vector<std::unique_ptr<Constraint>> &constraints, bool all_varchar) {
    try {
        // Get column information using nanodbc catalog
        nanodbc::catalog catalog(conn);
        auto column_results = catalog.find_columns(std::string(), std::string(), table_name, std::string());
        
        idx_t column_index = 0;
        std::vector<idx_t> not_null_columns;
        
        while (column_results.next()) {
            std::string name = column_results.column_name();
            SQLSMALLINT data_type = column_results.data_type();
            SQLULEN column_size = column_results.column_size();
            SQLSMALLINT decimal_digits = column_results.decimal_digits();
            SQLSMALLINT nullable = column_results.nullable();
            
            LogicalType column_type;
            
            if (all_varchar) {
                column_type = LogicalType::VARCHAR;
            } else {
                column_type = NanodbcUtils::TypeToLogicalType(data_type, column_size, decimal_digits);
            }
            
            ColumnDefinition column(std::move(name), column_type);
            columns.AddColumn(std::move(column));
            
            if (nullable == SQL_NO_NULLS) {
                constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(column_index)));
            }
            
            column_index++;
        }
        
        if (column_index == 0) {
            throw std::runtime_error("No columns found for table '" + table_name + "'");
        }
        
        // Get primary key information
        auto pk_results = catalog.find_primary_keys(std::string(), std::string(), table_name);
        std::vector<std::string> primary_keys;
        
        while (pk_results.next()) {
            // Use column_name() method instead of get<std::string>()
            std::string pk_name = pk_results.column_name();
            primary_keys.push_back(pk_name);
        }
        
        if (!primary_keys.empty()) {
            if (primary_keys.size() == 1) {
                // Find the column index of this primary key
                for (idx_t i = 0; i < columns.LogicalColumnCount(); i++) {
                    if (columns.GetColumn(LogicalIndex(i)).GetName() == primary_keys[0]) {
                        constraints.push_back(make_uniq<UniqueConstraint>(LogicalIndex(i), true));
                        break;
                    }
                }
            } else {
                constraints.push_back(make_uniq<UniqueConstraint>(std::move(primary_keys), true));
            }
        }
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get table info for '" + table_name + "': " + NanodbcUtils::HandleException(e));
    }
}

bool NanodbcDB::ColumnExists(const std::string &table_name, const std::string &column_name) {
    try {
        nanodbc::catalog catalog(conn);
        auto column_results = catalog.find_columns(std::string(), std::string(), table_name, column_name);
        
        return column_results.next();
    } catch (const nanodbc::database_error&) {
        return false;
    }
}

CatalogType NanodbcDB::GetEntryType(const std::string &name) {
    try {
        nanodbc::catalog catalog(conn);
        
        // Check if it's a table
        auto table_results = catalog.find_tables(std::string(), std::string(), name, std::string("TABLE"));
        if (table_results.next()) {
            return CatalogType::TABLE_ENTRY;
        }
        
        // Check if it's a view
        auto view_results = catalog.find_tables(std::string(), std::string(), name, std::string("VIEW"));
        if (view_results.next()) {
            return CatalogType::VIEW_ENTRY;
        }
    } catch (const nanodbc::database_error&) {
        // Ignore exceptions during type check
    }
    
    return CatalogType::INVALID;
}

void NanodbcDB::DebugSetPrintQueries(bool print) {
    debug_print_queries = print;
}

} // namespace duckdb