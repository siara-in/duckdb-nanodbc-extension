#include "odbc_connection.hpp"
#include "odbc_statement.hpp"
#include "odbc_utils.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

namespace duckdb {

bool OdbcConnection::debugPrintQueries = false;

OdbcConnection::OdbcConnection() : owner(false) {
}

OdbcConnection::~OdbcConnection() {
    Close();
}

OdbcConnection::OdbcConnection(OdbcConnection &&other) noexcept {
    connection = std::move(other.connection);
    owner = other.owner;
    other.owner = false;
}

OdbcConnection &OdbcConnection::operator=(OdbcConnection &&other) noexcept {
    if (this != &other) {
        Close();
        connection = std::move(other.connection);
        owner = other.owner;
        other.owner = false;
    }
    return *this;
}

OdbcConnection OdbcConnection::Connect(const ConnectionParams& params) {
    try {
        OdbcConnection db;
        
        // Connect to ODBC data source
        if (!params.Dsn.empty()) {
            // Connect via DSN
            if (params.Username.empty() && params.Password.empty()) {
                db.connection = nanodbc::connection(params.Dsn, "", "", params.Timeout);
            } else {
                db.connection = nanodbc::connection(params.Dsn, params.Username, params.Password, params.Timeout);
            }
        } else if (!params.ConnectionString.empty()) {
            // Connect via connection string
            db.connection = nanodbc::connection(params.ConnectionString, params.Timeout);
        } else {
            throw std::runtime_error("No connection information provided");
        }
        
        // Set read-only mode if requested
        if (params.ReadOnly) {
            try {
                // Use native ODBC call for setting connection attributes
                SQLHDBC nativeHandle = db.connection.native_dbc_handle();
                SQLSetConnectAttr(nativeHandle, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);
            } catch (const std::exception& e) {
                // Just log warning, don't fail if read-only setting is not supported
                fprintf(stderr, "Warning: Failed to set read-only mode: %s\n", e.what());
            }
        }
        
        db.owner = true;
        return db;
    } catch (const nanodbc::database_error& e) {
        if (!params.Dsn.empty()) {
            throw std::runtime_error(OdbcUtils::FormatError("connect to DSN '" + params.Dsn + "'", e));
        } else {
            throw std::runtime_error(OdbcUtils::FormatError("connect with connection string", e));
        }
    }
}

OdbcStatement OdbcConnection::Prepare(const std::string &query) {
    if (!IsOpen()) {
        throw std::runtime_error("Cannot prepare statement: connection is closed");
    }
    
    try {
        return OdbcStatement(connection, query);
    } catch (const nanodbc::database_error &e) {
        throw std::runtime_error(OdbcUtils::FormatError("prepare query \"" + query + "\"", e));
    }
}

void OdbcConnection::Execute(const std::string &query) {
    if (debugPrintQueries) {
        printf("ODBC Query: %s\n", query.c_str());
    }
    
    try {
        nanodbc::just_execute(connection, query);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("execute query \"" + query + "\"", e));
    }
}

bool OdbcConnection::IsOpen() const {
    return connection.connected();
}

void OdbcConnection::Close() {
    if (!owner) {
        return;
    }
    
    if (IsOpen()) {
        try {
            connection.disconnect();
        } catch (...) {
            // Ignore exceptions during disconnect
        }
    }
    
    owner = false;
}

std::vector<std::string> OdbcConnection::GetTables() {
    std::vector<std::string> tables;
    
    try {
        // Use nanodbc's catalog functions to get tables
        nanodbc::catalog catalog(connection);
        auto tableResults = catalog.find_tables(std::string(), std::string("TABLE"), std::string(), std::string());
        
        while (tableResults.next()) {
            std::string tableName = tableResults.table_name();
            tables.push_back(tableName);
        }
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get table list", e));
    }
    
    return tables;
}

void OdbcConnection::GetTableInfo(const std::string &tableName, ColumnList &columns, 
                                std::vector<std::unique_ptr<Constraint>> &constraints, bool allVarchar) {
    try {
        // Get column information using nanodbc catalog
        nanodbc::catalog catalog(connection);
        auto columnResults = catalog.find_columns(std::string(), tableName, std::string(), std::string());
        
        idx_t columnIndex = 0;
        
        while (columnResults.next()) {
            std::string name = columnResults.column_name();
            SQLSMALLINT dataType = columnResults.data_type();
            SQLULEN columnSize = columnResults.column_size();
            SQLSMALLINT decimalDigits = columnResults.decimal_digits();
            SQLSMALLINT nullable = columnResults.nullable();
            
            LogicalType columnType;
            
            if (allVarchar) {
                columnType = LogicalType::VARCHAR;
            } else {
                columnType = OdbcUtils::ToLogicalType(dataType, columnSize, decimalDigits);
            }
            
            ColumnDefinition column(std::move(name), columnType);
            columns.AddColumn(std::move(column));
            
            if (nullable == SQL_NO_NULLS) {
                constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(columnIndex)));
            }
            
            columnIndex++;
        }
        
        if (columnIndex == 0) {
            throw std::runtime_error("No columns found for table '" + tableName + "'");
        }
        
        // Get primary key information
        auto pkResults = catalog.find_primary_keys(tableName, std::string(), std::string());
        std::vector<std::string> primaryKeys;
        
        while (pkResults.next()) {
            std::string pkName = pkResults.column_name();
            primaryKeys.push_back(pkName);
        }
        
        if (!primaryKeys.empty()) {
            if (primaryKeys.size() == 1) {
                // Single-column primary key
                for (idx_t i = 0; i < columns.LogicalColumnCount(); i++) {
                    if (columns.GetColumn(LogicalIndex(i)).GetName() == primaryKeys[0]) {
                        constraints.push_back(make_uniq<UniqueConstraint>(LogicalIndex(i), true));
                        break;
                    }
                }
            } else {
                // Multi-column primary key
                constraints.push_back(make_uniq<UniqueConstraint>(std::move(primaryKeys), true));
            }
        }
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get table info for '" + tableName + "'", e));
    }
}

bool OdbcConnection::ColumnExists(const std::string &tableName, const std::string &columnName) {
    try {
        nanodbc::catalog catalog(connection);
        auto columnResults = catalog.find_columns(columnName, tableName, std::string(), std::string());
        
        return columnResults.next();
    } catch (const nanodbc::database_error&) {
        return false;
    }
}

CatalogType OdbcConnection::GetEntryType(const std::string &name) {
    try {
        nanodbc::catalog catalog(connection);
        
        // Check if it's a table
        auto tableResults = catalog.find_tables(name, std::string("TABLE"), std::string(), std::string());
        if (tableResults.next()) {
            return CatalogType::TABLE_ENTRY;
        }
        
        // Check if it's a view
        auto viewResults = catalog.find_tables(name, std::string("VIEW"), std::string(), std::string());
        if (viewResults.next()) {
            return CatalogType::VIEW_ENTRY;
        }
    } catch (const nanodbc::database_error&) {
        // Ignore exceptions during type check
    }
    
    return CatalogType::INVALID;
}

void OdbcConnection::SetDebugPrintQueries(bool print) {
    debugPrintQueries = print;
}

} // namespace duckdb