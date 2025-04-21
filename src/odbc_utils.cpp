#include "odbc_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

std::string OdbcUtils::FormatError(const std::string& operation, const nanodbc::database_error& e) {
    return "Failed to " + operation + ": " + e.what();
}

std::string OdbcUtils::TypeToString(SQLSMALLINT odbcType) {
    static const std::unordered_map<SQLSMALLINT, std::string> typeNames = {
        {SQL_CHAR, "CHAR"},
        {SQL_VARCHAR, "VARCHAR"},
        {SQL_LONGVARCHAR, "LONGVARCHAR"},
        {SQL_WCHAR, "WCHAR"},
        {SQL_WVARCHAR, "WVARCHAR"},
        {SQL_WLONGVARCHAR, "WLONGVARCHAR"},
        {SQL_DECIMAL, "DECIMAL"},
        {SQL_NUMERIC, "NUMERIC"},
        {SQL_SMALLINT, "SMALLINT"},
        {SQL_INTEGER, "INTEGER"},
        {SQL_REAL, "REAL"},
        {SQL_FLOAT, "FLOAT"},
        {SQL_DOUBLE, "DOUBLE"},
        {SQL_BIT, "BIT"},
        {SQL_TINYINT, "TINYINT"},
        {SQL_BIGINT, "BIGINT"},
        {SQL_BINARY, "BINARY"},
        {SQL_VARBINARY, "VARBINARY"},
        {SQL_LONGVARBINARY, "LONGVARBINARY"},
        {SQL_DATE, "DATE"},
        {SQL_TIME, "TIME"},
        {SQL_TIMESTAMP, "TIMESTAMP"},
        {SQL_GUID, "GUID"}
    };
    
    auto it = typeNames.find(odbcType);
    if (it != typeNames.end()) {
        return it->second;
    }
    return "UNKNOWN";
}

std::string OdbcUtils::SanitizeString(const std::string& input) {
    return StringUtil::Replace(input, "\"", "\"\"");
}

SQLSMALLINT OdbcUtils::ToOdbcType(const LogicalType& type) {
    static const std::unordered_map<LogicalTypeId, SQLSMALLINT> typeMap = {
        {LogicalTypeId::BOOLEAN, SQL_BIT},
        {LogicalTypeId::TINYINT, SQL_TINYINT},
        {LogicalTypeId::SMALLINT, SQL_SMALLINT},
        {LogicalTypeId::INTEGER, SQL_INTEGER},
        {LogicalTypeId::BIGINT, SQL_BIGINT},
        {LogicalTypeId::FLOAT, SQL_REAL},
        {LogicalTypeId::DOUBLE, SQL_DOUBLE},
        {LogicalTypeId::VARCHAR, SQL_VARCHAR},
        {LogicalTypeId::BLOB, SQL_VARBINARY},
        {LogicalTypeId::TIMESTAMP, SQL_TIMESTAMP},
        {LogicalTypeId::DATE, SQL_DATE},
        {LogicalTypeId::TIME, SQL_TIME},
        {LogicalTypeId::DECIMAL, SQL_DECIMAL},
        {LogicalTypeId::UTINYINT, SQL_TINYINT},
        {LogicalTypeId::USMALLINT, SQL_SMALLINT},
        {LogicalTypeId::UINTEGER, SQL_INTEGER},
        {LogicalTypeId::UBIGINT, SQL_BIGINT}
    };
    
    auto it = typeMap.find(type.id());
    if (it != typeMap.end()) {
        return it->second;
    }
    return SQL_VARCHAR; // Default to VARCHAR for unknown types
}

LogicalType OdbcUtils::ToLogicalType(SQLSMALLINT odbcType, SQLULEN columnSize, SQLSMALLINT decimalDigits) {
    switch (odbcType) {
        case SQL_BIT:
#ifdef SQL_BOOLEAN
        case SQL_BOOLEAN:
#endif
            return LogicalType(LogicalTypeId::BOOLEAN);
            
        case SQL_TINYINT:
            return LogicalType::TINYINT;
            
        case SQL_SMALLINT:
            return LogicalType::SMALLINT;
            
        case SQL_INTEGER:
            return LogicalType::INTEGER;
            
        case SQL_BIGINT:
            return LogicalType::BIGINT;
            
        case SQL_REAL:
        case SQL_FLOAT:
            return LogicalType::FLOAT;
            
        case SQL_DOUBLE:
            return LogicalType::DOUBLE;
            
        case SQL_DECIMAL:
        case SQL_NUMERIC:
            return LogicalType::DECIMAL(columnSize, decimalDigits);
            
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:
            return LogicalType::VARCHAR;
            
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
            return LogicalType::BLOB;
            
        case SQL_DATE:
        case SQL_TYPE_DATE:
            return LogicalType::DATE;
            
        case SQL_TYPE_TIME:    
        case SQL_TIME:
            return LogicalType::TIME;
            
        case SQL_TYPE_TIMESTAMP:    
        case SQL_TIMESTAMP:
            return LogicalType::TIMESTAMP;
            
        case SQL_GUID:
            return LogicalType::UUID;
            
        default:
            return LogicalType::VARCHAR;
    }
}

bool OdbcUtils::IsBinaryType(SQLSMALLINT sqlType) {
    return sqlType == SQL_BINARY || sqlType == SQL_VARBINARY || sqlType == SQL_LONGVARBINARY;
}

bool OdbcUtils::IsWideType(SQLSMALLINT sqlType) {
    return sqlType == SQL_WCHAR || sqlType == SQL_WVARCHAR || sqlType == SQL_WLONGVARCHAR;
}

bool OdbcUtils::ReadVarData(nanodbc::result& result, idx_t colIdx, bool& isNull, std::vector<char>& output) {
    isNull = false;
    output.clear();
    
    try {
        if (result.is_null(colIdx)) {
            isNull = true;
            return true;
        }
        
        // Get the data using nanodbc
        std::string value = result.get<std::string>(colIdx);
        
        // Copy to output vector
        output.assign(value.begin(), value.end());
        return true;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(FormatError("read variable data", e));
    }
}

void OdbcUtils::GetColumnMetadata(nanodbc::result& result, idx_t colIdx, 
                                 SQLSMALLINT& type, SQLULEN& columnSize, SQLSMALLINT& decimalDigits) {
    try {
        // Get the column data type
        type = result.column_datatype(colIdx);
        
        // Get the column size and decimal digits
        columnSize = 0;
        decimalDigits = 0;
        
        // For some data types, we need additional metadata
        if (type == SQL_NUMERIC || type == SQL_DECIMAL) {
            columnSize = result.column_size(colIdx);
            decimalDigits = result.column_decimal_digits(colIdx);
        } else if (type == SQL_CHAR || type == SQL_VARCHAR || type == SQL_WCHAR || type == SQL_WVARCHAR) {
            columnSize = result.column_size(colIdx);
        } else if (type == SQL_BINARY || type == SQL_VARBINARY) {
            columnSize = result.column_size(colIdx);
        }
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(FormatError("get column metadata", e));
    }
}

} // namespace duckdb