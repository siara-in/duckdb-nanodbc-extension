#include "odbc_utils.hpp"

namespace duckdb {

void ODBCUtils::Check(SQLRETURN rc, SQLSMALLINT handle_type, SQLHANDLE handle, const std::string &operation) {
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        std::string error_message = GetErrorMessage(handle_type, handle);
        throw std::runtime_error("ODBC Error in " + operation + ": " + error_message);
    }
}

std::string ODBCUtils::GetErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle) {
    SQLCHAR sql_state[6];
    SQLINTEGER native_error;
    SQLCHAR message_text[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT text_length;
    
    SQLRETURN ret = SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_error, 
                                 message_text, sizeof(message_text), &text_length);
    
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return std::string((char*)message_text, text_length);
    } else {
        return "Unknown error";
    }
}

std::string ODBCUtils::TypeToString(SQLSMALLINT odbc_type) {
    switch (odbc_type) {
        case SQL_CHAR:         return "CHAR";
        case SQL_VARCHAR:      return "VARCHAR";
        case SQL_LONGVARCHAR:  return "LONGVARCHAR";
        case SQL_WCHAR:        return "WCHAR";
        case SQL_WVARCHAR:     return "WVARCHAR";
        case SQL_WLONGVARCHAR: return "WLONGVARCHAR";
        case SQL_DECIMAL:      return "DECIMAL";
        case SQL_NUMERIC:      return "NUMERIC";
        case SQL_SMALLINT:     return "SMALLINT";
        case SQL_INTEGER:      return "INTEGER";
        case SQL_REAL:         return "REAL";
        case SQL_FLOAT:        return "FLOAT";
        case SQL_DOUBLE:       return "DOUBLE";
        case SQL_BIT:          return "BIT";
        case SQL_TINYINT:      return "TINYINT";
        case SQL_BIGINT:       return "BIGINT";
        case SQL_BINARY:       return "BINARY";
        case SQL_VARBINARY:    return "VARBINARY";
        case SQL_LONGVARBINARY:return "LONGVARBINARY";
        case SQL_DATE:         return "DATE";
        case SQL_TIME:         return "TIME";
        case SQL_TIMESTAMP:    return "TIMESTAMP";
        case SQL_GUID:         return "GUID";
        default:               return "UNKNOWN";
    }
}

std::string ODBCUtils::SanitizeString(const std::string &input) {
    return StringUtil::Replace(input, "\"", "\"\"");
}

SQLSMALLINT ODBCUtils::ToODBCType(const LogicalType &input) {
    switch (input.id()) {
        case LogicalTypeId::BOOLEAN:     return SQL_BIT;
        case LogicalTypeId::TINYINT:     return SQL_TINYINT;
        case LogicalTypeId::SMALLINT:    return SQL_SMALLINT;
        case LogicalTypeId::INTEGER:     return SQL_INTEGER;
        case LogicalTypeId::BIGINT:      return SQL_BIGINT;
        case LogicalTypeId::FLOAT:       return SQL_REAL;
        case LogicalTypeId::DOUBLE:      return SQL_DOUBLE;
        case LogicalTypeId::VARCHAR:     return SQL_VARCHAR;
        case LogicalTypeId::BLOB:        return SQL_VARBINARY;
        case LogicalTypeId::TIMESTAMP:   return SQL_TIMESTAMP;
        case LogicalTypeId::DATE:        return SQL_DATE;
        case LogicalTypeId::TIME:        return SQL_TIME;
        case LogicalTypeId::DECIMAL:     return SQL_DECIMAL;
        case LogicalTypeId::UTINYINT:    return SQL_TINYINT;
        case LogicalTypeId::USMALLINT:   return SQL_SMALLINT;
        case LogicalTypeId::UINTEGER:    return SQL_INTEGER;
        case LogicalTypeId::UBIGINT:     return SQL_BIGINT;
        case LogicalTypeId::HUGEINT:     return SQL_VARCHAR; // Convert to string for HUGEINT
        case LogicalTypeId::LIST:        return SQL_VARCHAR; // Serialize lists as strings
        case LogicalTypeId::STRUCT:      return SQL_VARCHAR; // Serialize structs as strings
        case LogicalTypeId::MAP:         return SQL_VARCHAR; // Serialize maps as strings
        default:                         return SQL_VARCHAR; // Default to VARCHAR for unknown types
    }
}

LogicalType ODBCUtils::TypeToLogicalType(SQLSMALLINT odbc_type, SQLULEN column_size, SQLSMALLINT decimal_digits) {
    switch (odbc_type) {
        case SQL_BIT:
#ifdef SQL_BOOLEAN
        case SQL_BOOLEAN:
#endif
            return LogicalType::BOOLEAN;
            
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
            if (decimal_digits == 0) {
                if (column_size <= 2) {
                    return LogicalType::TINYINT;
                } else if (column_size <= 4) {
                    return LogicalType::SMALLINT;
                } else if (column_size <= 9) {
                    return LogicalType::INTEGER;
                } else if (column_size <= 18) {
                    return LogicalType::BIGINT;
                } else {
                    return LogicalType::DOUBLE;
                }
            } else {
                return LogicalType::DOUBLE;
            }
            
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
            return LogicalType::DATE;
            
        case SQL_TIME:
            return LogicalType::TIME;
            
        case SQL_TIMESTAMP:
            return LogicalType::TIMESTAMP;
            
        case SQL_GUID:
            return LogicalType::VARCHAR;
            
        default:
            return LogicalType::VARCHAR;
    }
}

} // namespace duckdb