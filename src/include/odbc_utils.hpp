#pragma once

#include "duckdb.hpp"
#include "odbc_headers.hpp"
#include <string>
#include <unordered_map>

namespace duckdb {

class OdbcUtils {
public:
    // Format error messages from nanodbc exceptions
    static std::string FormatError(const std::string& operation, const nanodbc::database_error& e);
    
    // Convert ODBC type to string representation
    static std::string TypeToString(SQLSMALLINT odbcType);
    
    // Sanitize string for ODBC usage (escape quotes)
    static std::string SanitizeString(const std::string& input);
    
    // Convert DuckDB type to ODBC type
    static SQLSMALLINT ToOdbcType(const LogicalType& type);
    
    // Convert ODBC type to DuckDB LogicalType
    static LogicalType ToLogicalType(SQLSMALLINT odbcType, SQLULEN columnSize, SQLSMALLINT decimalDigits);
    
    // Helper to determine if a type is binary
    static bool IsBinaryType(SQLSMALLINT sqlType);
    
    // Helper to determine if a type is wide (Unicode)
    static bool IsWideType(SQLSMALLINT sqlType);
    
    // Read variable length column data from nanodbc result
    static bool ReadVarData(nanodbc::result& result, idx_t colIdx, bool& isNull, std::vector<char>& output);
    
    // Get column metadata from nanodbc result
    static void GetColumnMetadata(nanodbc::result& result, idx_t colIdx, 
                                 SQLSMALLINT& type, SQLULEN& columnSize, SQLSMALLINT& decimalDigits);
};

} // namespace duckdb