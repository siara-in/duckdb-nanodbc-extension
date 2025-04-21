#pragma once

#include "duckdb.hpp"
#include "odbc_headers.hpp"
#include <type_traits>

namespace duckdb {

// Forward declarations
struct ScannerState;

class OdbcStatement {
public:
    OdbcStatement();
    OdbcStatement(nanodbc::connection& conn, const std::string& query);
    ~OdbcStatement();

    // Move semantics
    OdbcStatement(OdbcStatement &&other) noexcept;
    OdbcStatement &operator=(OdbcStatement &&other) noexcept;

    // Forbid copying
    OdbcStatement(const OdbcStatement &) = delete;
    OdbcStatement &operator=(const OdbcStatement &) = delete;

    // Execute and fetch next row
    bool Step();
    
    // Reset statement for re-execution
    void Reset();
    
    // Close statement
    void Close();
    
    // Check if statement is open
    bool IsOpen() const;
    
    // Get column metadata
    SQLSMALLINT GetOdbcType(idx_t colIdx, SQLULEN* columnSize = nullptr, SQLSMALLINT* decimalDigits = nullptr);
    std::string GetName(idx_t colIdx);
    idx_t GetColumnCount();
    SQLLEN GetValueLength(idx_t colIdx);
    
    // Get value from result set with type safety
    template <typename T>
    T GetValue(idx_t colIdx);
    
    // Bind parameter with type safety
    template <typename T>
    void Bind(idx_t colIdx, T value);
    
    // Specialized binding methods for blobs and strings
    void BindBlob(idx_t colIdx, const string_t &value);
    void BindText(idx_t colIdx, const string_t &value);
    
    // Bind a value from a vector
    void BindValue(Vector &col, idx_t colIdx, idx_t rowIdx);
    
    // Type checking functions
    void ValidateType(SQLLEN indicator, SQLSMALLINT odbcType, SQLSMALLINT expectedType, idx_t colIdx);
    void ValidateNumericType(SQLSMALLINT odbcType, idx_t colIdx);

    // Make result accessible to scanner
    nanodbc::statement stmt;
    nanodbc::result result;
    
private:
    bool hasResult;
    bool executed;
};

} // namespace duckdb