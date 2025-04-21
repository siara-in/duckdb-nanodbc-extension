#include "odbc_statement.hpp"
#include "odbc_utils.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"

namespace duckdb {

OdbcStatement::OdbcStatement() : hasResult(false), executed(false) {
}

OdbcStatement::OdbcStatement(nanodbc::connection &conn, const std::string &query)
    : hasResult(false), executed(false) {
    try {
        // Prepare the statement
        stmt = nanodbc::statement(conn, query);
    } catch (const nanodbc::database_error &e) {
        throw std::runtime_error(OdbcUtils::FormatError("prepare statement", e));
    }
}

OdbcStatement::~OdbcStatement() {
    Close();
}

OdbcStatement::OdbcStatement(OdbcStatement &&other) noexcept
    : stmt(std::move(other.stmt)),
      result(std::move(other.result)),
      hasResult(other.hasResult),
      executed(other.executed) {
    // Reset the moved-from instance
    other.stmt = nanodbc::statement();
    other.result = nanodbc::result();
    other.hasResult = false;
    other.executed = false;
}

OdbcStatement &OdbcStatement::operator=(OdbcStatement &&other) noexcept {
    if (this != &other) {
        // Clean up current handles
        Close();
        // Move in the new handles
        stmt = std::move(other.stmt);
        result = std::move(other.result);
        hasResult = other.hasResult;
        executed = other.executed;
        // Reset the moved-from object
        other.stmt = nanodbc::statement();
        other.result = nanodbc::result();
        other.hasResult = false;
        other.executed = false;
    }
    return *this;
}

bool OdbcStatement::Step() {
    if (!IsOpen()) {
        return false;
    }
    try {
        // On the first call, execute; on subsequent calls, advance the cursor
        if (!executed) {
            result = stmt.execute();
            executed = true;
            hasResult = true;
        }
        
        // The first call to next() moves to the first row
        return result.next();
    } catch (const nanodbc::database_error &e) {
        throw std::runtime_error(OdbcUtils::FormatError("execute statement", e));
    }
}

void OdbcStatement::Reset() {
    if (IsOpen()) {
        try {
            stmt.close();
            hasResult = false;
            executed = false;
        } catch (const nanodbc::database_error& e) {
            throw std::runtime_error(OdbcUtils::FormatError("reset statement", e));
        }
    }
}

void OdbcStatement::Close() {
    if (IsOpen()) {
        try {
            stmt.close();
            hasResult = false;
            executed = false;
        } catch (...) {
            // Ignore exceptions during close
        }
    }
}

bool OdbcStatement::IsOpen() const {
    return stmt.connected();
}

SQLSMALLINT OdbcStatement::GetOdbcType(idx_t colIdx, SQLULEN* columnSize, SQLSMALLINT* decimalDigits) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        if (!executed) {
            // Execute to get metadata
            result = stmt.execute();
            executed = true;
            hasResult = true;
        }
        
        SQLSMALLINT dataType;
        SQLULEN size;
        SQLSMALLINT digits;
        
        // Get column metadata
        OdbcUtils::GetColumnMetadata(result, colIdx, dataType, size, digits);
        
        // Pass back column size and decimal digits if requested
        if (columnSize) *columnSize = size;
        if (decimalDigits) *decimalDigits = digits;
        
        return dataType;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get column type", e));
    }
}

std::string OdbcStatement::GetName(idx_t colIdx) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        if (!executed) {
            // Execute to get metadata
            result = stmt.execute();
            executed = true;
            hasResult = true;
        }
        
        return result.column_name(colIdx);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get column name", e));
    }
}

idx_t OdbcStatement::GetColumnCount() {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        if (!executed) {
            // Execute to get metadata
            result = stmt.execute();
            executed = true;
            hasResult = true;
        }
        
        return result.columns();
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get column count", e));
    }
}

SQLLEN OdbcStatement::GetValueLength(idx_t colIdx) {
    if (!IsOpen() || !hasResult) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(colIdx)) {
            return SQL_NULL_DATA;
        }
        
        SQLSMALLINT type;
        SQLULEN columnSize;
        SQLSMALLINT decimalDigits;
        OdbcUtils::GetColumnMetadata(result, colIdx, type, columnSize, decimalDigits);
        
        // For variable length data like strings and blobs
        if (OdbcUtils::IsBinaryType(type) || type == SQL_VARCHAR || type == SQL_CHAR || 
            type == SQL_WVARCHAR || type == SQL_WCHAR) {
            std::string value = result.get<std::string>(colIdx);
            return value.length();
        }
        
        // For fixed length data, return the column size
        return columnSize;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get value length", e));
    }
}

// Template specializations for GetValue
template <>
std::string OdbcStatement::GetValue(idx_t colIdx) {
    if (!IsOpen() || !hasResult) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(colIdx)) {
            return std::string();
        }
        
        return result.get<std::string>(colIdx);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get string value", e));
    }
}

template <>
int OdbcStatement::GetValue(idx_t colIdx) {
    if (!IsOpen() || !hasResult) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(colIdx)) {
            return 0;
        }
        
        return result.get<int>(colIdx);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get int value", e));
    }
}

template <>
int64_t OdbcStatement::GetValue(idx_t colIdx) {
    if (!IsOpen() || !hasResult) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(colIdx)) {
            return 0;
        }
        
        return result.get<int64_t>(colIdx);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get int64 value", e));
    }
}

template <>
double OdbcStatement::GetValue(idx_t colIdx) {
    if (!IsOpen() || !hasResult) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(colIdx)) {
            return 0.0;
        }
        
        return result.get<double>(colIdx);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get double value", e));
    }
}

template <>
timestamp_t OdbcStatement::GetValue(idx_t colIdx) {
    if (!IsOpen() || !hasResult) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(colIdx)) {
            // Return epoch for null
            return Timestamp::FromEpochSeconds(0);
        }
        
        // Get timestamp using nanodbc
        nanodbc::timestamp ts = result.get<nanodbc::timestamp>(colIdx);
        
        // Convert to DuckDB timestamp
        date_t date = Date::FromDate(ts.year, ts.month, ts.day);
        dtime_t time = Time::FromTime(ts.hour, ts.min, ts.sec, ts.fract / 1000000);
        return Timestamp::FromDatetime(date, time);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("get timestamp value", e));
    }
}

// Template specializations for Bind
template <>
void OdbcStatement::Bind(idx_t colIdx, int value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        stmt.bind(colIdx, &value);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("bind int parameter", e));
    }
}

template <>
void OdbcStatement::Bind(idx_t colIdx, int64_t value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        stmt.bind(colIdx, &value);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("bind int64 parameter", e));
    }
}

template <>
void OdbcStatement::Bind(idx_t colIdx, double value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        stmt.bind(colIdx, &value);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("bind double parameter", e));
    }
}

template <>
void OdbcStatement::Bind(idx_t colIdx, std::nullptr_t) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        stmt.bind_null(colIdx);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("bind null parameter", e));
    }
}

void OdbcStatement::BindBlob(idx_t colIdx, const string_t &value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        // Bind binary data
        stmt.bind(colIdx, value.GetDataUnsafe(), value.GetSize());
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("bind blob parameter", e));
    }
}

void OdbcStatement::BindText(idx_t colIdx, const string_t &value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        std::string str(value.GetDataUnsafe(), value.GetSize());
        stmt.bind(colIdx, str.c_str());
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error(OdbcUtils::FormatError("bind text parameter", e));
    }
}

void OdbcStatement::BindValue(Vector &col, idx_t colIdx, idx_t rowIdx) {
    auto &mask = FlatVector::Validity(col);
    if (!mask.RowIsValid(rowIdx)) {
        Bind<std::nullptr_t>(colIdx, nullptr);
    } else {
        switch (col.GetType().id()) {
            case LogicalTypeId::BIGINT:
                Bind<int64_t>(colIdx, FlatVector::GetData<int64_t>(col)[rowIdx]);
                break;
            case LogicalTypeId::INTEGER:
                Bind<int>(colIdx, FlatVector::GetData<int>(col)[rowIdx]);
                break;
            case LogicalTypeId::DOUBLE:
                Bind<double>(colIdx, FlatVector::GetData<double>(col)[rowIdx]);
                break;
            case LogicalTypeId::BLOB:
                BindBlob(colIdx, FlatVector::GetData<string_t>(col)[rowIdx]);
                break;
            case LogicalTypeId::VARCHAR:
                BindText(colIdx, FlatVector::GetData<string_t>(col)[rowIdx]);
                break;
            default:
                throw std::runtime_error("Unsupported type for binding: " + col.GetType().ToString());
        }
    }
}

void OdbcStatement::ValidateType(SQLLEN indicator, SQLSMALLINT odbcType, SQLSMALLINT expectedType, idx_t colIdx) {
    if (indicator == SQL_NULL_DATA) {
        // Null values don't need type checking
        return;
    }
    
    if (odbcType != expectedType) {
        std::string columnName = GetName(colIdx);
        std::string message = "Invalid type in column \"" + columnName + "\": column was declared as " +
                            OdbcUtils::TypeToString(expectedType) + ", found " +
                            OdbcUtils::TypeToString(odbcType) + " instead.";
        message += "\n* SET odbc_all_varchar=true to load all columns as VARCHAR "
                "and skip type conversions";
        throw std::runtime_error(message);
    }
}

void OdbcStatement::ValidateNumericType(SQLSMALLINT odbcType, idx_t colIdx) {
    if (odbcType != SQL_FLOAT && odbcType != SQL_DOUBLE && odbcType != SQL_REAL &&
        odbcType != SQL_INTEGER && odbcType != SQL_SMALLINT && odbcType != SQL_TINYINT && 
        odbcType != SQL_BIGINT && odbcType != SQL_DECIMAL && odbcType != SQL_NUMERIC) {
        std::string columnName = GetName(colIdx);
        std::string message = "Invalid type in column \"" + columnName + "\": expected float or integer, found " +
                            OdbcUtils::TypeToString(odbcType) + " instead.";
        message += "\n* SET odbc_all_varchar=true to load all columns as VARCHAR "
                "and skip type conversions";
        throw std::runtime_error(message);
    }
}

} // namespace duckdb