#include "odbc_scanner.hpp"
#include "odbc_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<FunctionData> BindScan(ClientContext &context, TableFunctionBindInput &input,
                               vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ScannerState>();
    
    // Check which connection method to use
    if (input.inputs[0].type().id() == LogicalTypeId::VARCHAR) {
        // First argument is table name
        result->TableName = input.inputs[0].GetValue<string>();
        
        if (input.inputs.size() < 2) {
            throw BinderException("ODBC scan requires at least a table name and either a DSN or connection string");
        }
        
        if (input.inputs[1].type().id() == LogicalTypeId::VARCHAR) {
            // Second argument can be either DSN or connection string
            auto connStr = input.inputs[1].GetValue<string>();
            
            // Check if it's a DSN or connection string
            if (connStr.find('=') == string::npos) {
                // Likely a DSN
                result->Dsn = connStr;
                
                // Check for optional username and password
                if (input.inputs.size() >= 3) {
                    result->Username = input.inputs[2].GetValue<string>();
                }
                
                if (input.inputs.size() >= 4) {
                    result->Password = input.inputs[3].GetValue<string>();
                }
            } else {
                // Connection string
                result->ConnectionString = connStr;
            }
        } else {
            throw BinderException("Second argument to ODBC scan must be a VARCHAR (DSN or connection string)");
        }
    } else {
        throw BinderException("First argument to ODBC scan must be a VARCHAR (table name)");
    }
    
    // Process additional parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "all_varchar") {
            result->AllVarchar = BooleanValue::Get(kv.second);
        }
    }
    
    // Connect to data source and get table schema
    try {
        ConnectionParams params;
        if (!result->Dsn.empty()) {
            params = ConnectionParams::FromDsn(result->Dsn, result->Username, result->Password);
        } else if (!result->ConnectionString.empty()) {
            params = ConnectionParams::FromConnectionString(result->ConnectionString);
        } else {
            throw BinderException("Either DSN or connection string must be provided for ODBC scan");
        }
        
        OdbcConnection db = OdbcConnection::Connect(params);
        
        // Get table information
        ColumnList columns;
        std::vector<std::unique_ptr<Constraint>> constraints;
        db.GetTableInfo(result->TableName, columns, constraints, result->AllVarchar);
        
        // Map column types and names
        for (auto &column : columns.Logical()) {
            names.push_back(column.GetName());
            return_types.push_back(column.GetType());
        }
        
        if (names.empty()) {
            throw BinderException("No columns found for table " + result->TableName);
        }
        
        result->Names = names;
        result->Types = return_types;
    } catch (const nanodbc::database_error& e) {
        throw BinderException("ODBC error during binding: " + OdbcUtils::FormatError("connect", e));
    } catch (const std::exception& e) {
        throw BinderException(e.what());
    }
    
    return std::move(result);
}

unique_ptr<GlobalTableFunctionState> InitGlobalScanState(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<GlobalScanState>(1); // Single-threaded scan for now
}

unique_ptr<LocalTableFunctionState> InitLocalScanState(ExecutionContext &context, TableFunctionInitInput &input, 
                                                     GlobalTableFunctionState *global_state) {
    auto &bindData = input.bind_data->Cast<ScannerState>();
    auto result = make_uniq<LocalScanState>();
    
    result->ColumnIds = input.column_ids;
    
    // If we have a global database connection, use it
    result->Connection = bindData.GlobalConnection;
    
    try {
        if (!result->Connection) {
            // Create a new connection
            ConnectionParams params;
            if (!bindData.Dsn.empty()) {
                params = ConnectionParams::FromDsn(bindData.Dsn, bindData.Username, bindData.Password);
            } else if (!bindData.ConnectionString.empty()) {
                params = ConnectionParams::FromConnectionString(bindData.ConnectionString);
            } else {
                throw std::runtime_error("No connection information available");
            }
            
            result->OwnedConnection = OdbcConnection::Connect(params);
            result->Connection = &result->OwnedConnection;
        }
        
        // Prepare the query
        string sql;
        if (bindData.Sql.empty()) {
            // Build query based on column IDs
            auto colNames = StringUtil::Join(
                result->ColumnIds.data(), result->ColumnIds.size(), ", ", [&](const idx_t columnId) {
                    return columnId == (column_t)-1 ? "NULL"
                                                  : '"' + OdbcUtils::SanitizeString(bindData.Names[columnId]) + '"';
                });
                
            sql = StringUtil::Format("SELECT %s FROM \"%s\"", colNames, 
                                   OdbcUtils::SanitizeString(bindData.TableName));
        } else {
            sql = bindData.Sql;
        }
        
        result->Statement = result->Connection->Prepare(sql);
        result->Done = false;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("ODBC error during initialization: " + OdbcUtils::FormatError("initialize scanner", e));
    } catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
    
    return std::move(result);
}

void ScanOdbcTable(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.local_state->Cast<LocalScanState>();
    if (state.Done) {
        output.SetCardinality(0);
        return;
    }
    
    // Fetch rows and populate the DataChunk
    idx_t outIdx = 0;
    while (outIdx < STANDARD_VECTOR_SIZE) {
        if (!state.Statement.Step()) {
            // No more rows to fetch
            state.Done = true;
            break;
        }
        state.ScanCount++;
        
        try {
            // Process each column
            for (idx_t colIdx = 0; colIdx < output.ColumnCount(); colIdx++) {
                auto &outVec = output.data[colIdx];
                
                // Using nanodbc result to get data
                auto &result = state.Statement.result;
                
                // Check for NULL
                if (result.is_null(colIdx)) {
                    FlatVector::Validity(outVec).Set(outIdx, false);
                    continue;
                }
                
                // Based on the output vector type, convert and fetch the data
                switch (outVec.GetType().id()) {
                    case LogicalTypeId::BOOLEAN: {
                        FlatVector::GetData<bool>(outVec)[outIdx] = (result.get<int>(colIdx) != 0);
                        break;
                    }
                    case LogicalTypeId::TINYINT: {
                        FlatVector::GetData<int8_t>(outVec)[outIdx] = static_cast<int8_t>(result.get<int>(colIdx));
                        break;
                    }
                    case LogicalTypeId::SMALLINT: {
                        FlatVector::GetData<int16_t>(outVec)[outIdx] = result.get<int16_t>(colIdx);
                        break;
                    }
                    case LogicalTypeId::INTEGER: {
                        FlatVector::GetData<int32_t>(outVec)[outIdx] = result.get<int32_t>(colIdx);
                        break;
                    }
                    case LogicalTypeId::BIGINT: {
                        FlatVector::GetData<int64_t>(outVec)[outIdx] = result.get<int64_t>(colIdx);
                        break;
                    }
                    case LogicalTypeId::FLOAT: {
                        FlatVector::GetData<float>(outVec)[outIdx] = result.get<float>(colIdx);
                        break;
                    }
                    case LogicalTypeId::DOUBLE: {
                        FlatVector::GetData<double>(outVec)[outIdx] = result.get<double>(colIdx);
                        break;
                    }
                    case LogicalTypeId::DECIMAL: {
                        auto &decimalType = outVec.GetType();
                        
                        // Get precision and scale
                        uint8_t width = DecimalType::GetWidth(decimalType);
                        uint8_t scale = DecimalType::GetScale(decimalType);
                        
                        // Get column metadata for backup precision/scale
                        SQLSMALLINT odbcType;
                        SQLULEN columnSize;
                        SQLSMALLINT decimalDigits;
                        OdbcUtils::GetColumnMetadata(result, colIdx, odbcType, columnSize, decimalDigits);
                        
                        // Use backup values if needed
                        if (width == 0) width = columnSize;
                        if (scale == 0) scale = decimalDigits;
                        if (width == 0) width = 38;  // Default width
                        if (scale == 0) scale = 2;   // Default scale
                        
                        // Get value as string to preserve precision
                        std::string decimalStr = result.get<std::string>(colIdx);
                        
                        // Process based on target storage type
                        bool success = false;
                        switch (decimalType.InternalType()) {
                            case PhysicalType::INT16: {
                                int16_t resultVal;
                                success = TryDecimalStringCast<int16_t>(decimalStr.c_str(), decimalStr.length(), resultVal, width, scale);
                                if (success) {
                                    FlatVector::GetData<int16_t>(outVec)[outIdx] = resultVal;
                                }
                                break;
                            }
                            case PhysicalType::INT32: {
                                int32_t resultVal;
                                success = TryDecimalStringCast<int32_t>(decimalStr.c_str(), decimalStr.length(), resultVal, width, scale);
                                if (success) {
                                    FlatVector::GetData<int32_t>(outVec)[outIdx] = resultVal;
                                }
                                break;
                            }
                            case PhysicalType::INT64: {
                                int64_t resultVal;
                                success = TryDecimalStringCast<int64_t>(decimalStr.c_str(), decimalStr.length(), resultVal, width, scale);
                                if (success) {
                                    FlatVector::GetData<int64_t>(outVec)[outIdx] = resultVal;
                                }
                                break;
                            }
                            case PhysicalType::INT128: {
                                hugeint_t resultVal;
                                success = TryDecimalStringCast<hugeint_t>(decimalStr.c_str(), decimalStr.length(), resultVal, width, scale);
                                if (success) {
                                    FlatVector::GetData<hugeint_t>(outVec)[outIdx] = resultVal;
                                }
                                break;
                            }
                            default:
                                throw InternalException("Unsupported decimal storage type");
                        }
                        
                        if (!success) {
                            FlatVector::Validity(outVec).Set(outIdx, false);
                        }
                        break;
                    }
                    case LogicalTypeId::VARCHAR: {
                        std::string strVal = result.get<std::string>(colIdx);
                        FlatVector::GetData<string_t>(outVec)[outIdx] = 
                            StringVector::AddString(outVec, strVal);
                        break;
                    }
                    case LogicalTypeId::DATE: {
                        nanodbc::date dateVal = result.get<nanodbc::date>(colIdx);
                        FlatVector::GetData<date_t>(outVec)[outIdx] = Date::FromDate(dateVal.year, dateVal.month, dateVal.day);
                        break;
                    }
                    case LogicalTypeId::TIME: {
                        nanodbc::time timeVal = result.get<nanodbc::time>(colIdx);
                        FlatVector::GetData<dtime_t>(outVec)[outIdx] = Time::FromTime(timeVal.hour, timeVal.min, timeVal.sec, 0);
                        break;
                    }
                    case LogicalTypeId::TIMESTAMP: {
                        nanodbc::timestamp tsVal = result.get<nanodbc::timestamp>(colIdx);
                        date_t dateVal = Date::FromDate(tsVal.year, tsVal.month, tsVal.day);
                        dtime_t timeVal = Time::FromTime(tsVal.hour, tsVal.min, tsVal.sec, tsVal.fract / 1000000);
                        
                        FlatVector::GetData<timestamp_t>(outVec)[outIdx] = Timestamp::FromDatetime(dateVal, timeVal);
                        break;
                    }
                    case LogicalTypeId::UUID: {
                        // UUIDs are typically handled as strings in ODBC
                        std::string uuidStr = result.get<std::string>(colIdx);
                        
                        // Convert string UUID to DuckDB UUID format
                        try {
                            hugeint_t uuidValue;
                            if (UUID::FromString(uuidStr, uuidValue)) {
                                FlatVector::GetData<hugeint_t>(outVec)[outIdx] = uuidValue;
                            } else {
                                // If parsing fails, set to NULL
                                FlatVector::Validity(outVec).Set(outIdx, false);
                            }
                        } catch (...) {
                            // If any error occurs during conversion, set to NULL
                            FlatVector::Validity(outVec).Set(outIdx, false);
                        }
                        break;
                    }
                    case LogicalTypeId::BLOB: {
                        std::vector<char> blobData;
                        bool isNull = false;
                        
                        if (OdbcUtils::ReadVarData(result, colIdx, isNull, blobData)) {
                            if (isNull) {
                                FlatVector::Validity(outVec).Set(outIdx, false);
                            } else if (blobData.empty()) {
                                FlatVector::GetData<string_t>(outVec)[outIdx] = 
                                    StringVector::AddStringOrBlob(outVec, "", 0);
                            } else {
                                FlatVector::GetData<string_t>(outVec)[outIdx] = 
                                    StringVector::AddStringOrBlob(outVec, blobData.data(), blobData.size());
                            }
                        } else {
                            FlatVector::Validity(outVec).Set(outIdx, false);
                        }
                        break;
                    }
                    default:
                        throw std::runtime_error("Unsupported ODBC to DuckDB type conversion: " + 
                                               outVec.GetType().ToString());
                }
            }
        } catch (const nanodbc::database_error& e) {
            throw std::runtime_error("ODBC error during data scan: " + OdbcUtils::FormatError("scan data", e));
        } catch (const std::exception& e) {
            throw std::runtime_error(e.what());
        }
        
        outIdx++;
    }
    
    // Set the cardinality of the output chunk
    output.SetCardinality(outIdx);
}

unique_ptr<FunctionData> BindAttach(ClientContext &context, TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ScannerState>();
    
    // Check which connection method to use
    if (input.inputs[0].type().id() == LogicalTypeId::VARCHAR) {
        auto connStr = input.inputs[0].GetValue<string>();
        
        // Check if it's a DSN or connection string
        if (connStr.find('=') == string::npos) {
            // Likely a DSN
            result->Dsn = connStr;
            
            // Check for optional username and password
            if (input.inputs.size() >= 2) {
                result->Username = input.inputs[1].GetValue<string>();
            }
            
            if (input.inputs.size() >= 3) {
                result->Password = input.inputs[2].GetValue<string>();
            }
        } else {
            // Connection string
            result->ConnectionString = connStr;
        }
    } else {
        throw BinderException("First argument to ODBC attach must be a VARCHAR (DSN or connection string)");
    }
    
    // Store named parameters directly in ScannerState
    for (auto &kv : input.named_parameters) {
        result->named_parameters[kv.first] = kv.second;
    }
    
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
    names.emplace_back("Success");
    return std::move(result);
}

void AttachDatabase(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.bind_data->Cast<ScannerState>();
    static bool finished = false;
    
    if (finished) {
        return;
    }
    
    try {
        // Connect to the ODBC data source
        ConnectionParams params = state.CreateConnectionParams();
        OdbcConnection db = OdbcConnection::Connect(params);
        
        // Get list of tables
        auto tables = db.GetTables();
        
        // Create connection to DuckDB
        auto dconn = Connection(context.db->GetDatabase(context));
        
        // Process additional parameters
        bool overwrite = false;
        // Check named parameters directly from bind_data instead of accessing Table->parameters
        for (auto &kv : data.bind_data->Cast<ScannerState>().named_parameters) {
            if (kv.first == "overwrite") {
                overwrite = BooleanValue::Get(kv.second);
            }
        }
        
        // Create views for each table
        for (auto &tableName : tables) {
            if (!state.Dsn.empty()) {
                dconn.TableFunction("odbc_scan", {Value(tableName), Value(state.Dsn), 
                                  Value(state.Username), Value(state.Password)})
                    ->CreateView(tableName, overwrite, false);
            } else {
                dconn.TableFunction("odbc_scan", {Value(tableName), Value(state.ConnectionString)})
                    ->CreateView(tableName, overwrite, false);
            }
        }
        
        finished = true;
        
        // Set output
        output.SetCardinality(1);
        output.SetValue(0, 0, Value::BOOLEAN(true));
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("ODBC error during attach: " + OdbcUtils::FormatError("attach", e));
    } catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
}

unique_ptr<FunctionData> BindQuery(ClientContext &context, TableFunctionBindInput &input,
                                 vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ScannerState>();

    // NULL checks
    if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
        throw BinderException("Parameters to odbc_query cannot be NULL");
    }

    // Parse connection info
    auto connStr = input.inputs[0].GetValue<string>();
    if (connStr.find('=') == string::npos) {
        result->Dsn = connStr;
        if (input.inputs.size() >= 3) {
            result->Username = input.inputs[2].GetValue<string>();
        }
        if (input.inputs.size() >= 4) {
            result->Password = input.inputs[3].GetValue<string>();
        }
    } else {
        result->ConnectionString = connStr;
    }
    result->Sql = input.inputs[1].GetValue<string>();

    // all_varchar flag
    for (auto &kv : input.named_parameters) {
        if (kv.first == "all_varchar") {
            result->AllVarchar = BooleanValue::Get(kv.second);
        }
    }

    try {
        // Connect
        ConnectionParams params;
        if (!result->Dsn.empty()) {
            params = ConnectionParams::FromDsn(result->Dsn, result->Username, result->Password);
        } else {
            params = ConnectionParams::FromConnectionString(result->ConnectionString);
        }
        
        OdbcConnection db = OdbcConnection::Connect(params);

        // Prepare
        auto stmt = db.Prepare(result->Sql);
        if (!stmt.IsOpen()) {
            throw BinderException("Failed to prepare query");
        }

        // Step once to populate metadata
        bool hasData = stmt.Step();
        auto columnCount = stmt.GetColumnCount();

        // Pull column names/types
        for (idx_t i = 0; i < columnCount; i++) {
            auto colName = stmt.GetName(i);
            SQLULEN size = 0;
            SQLSMALLINT digits = 0;
            SQLSMALLINT odbcType = stmt.GetOdbcType(i, &size, &digits);

            auto duckType = result->AllVarchar 
                           ? LogicalType::VARCHAR
                           : OdbcUtils::ToLogicalType(odbcType, size, digits);

            names.push_back(colName);
            return_types.push_back(duckType);
        }

    } catch (const nanodbc::database_error &e) {
        throw BinderException("ODBC error during query bind: " + OdbcUtils::FormatError("bind query", e));
    } catch (const std::exception &e) {
        throw BinderException(e.what());
    }

    result->Names = names;
    result->Types = return_types;
    return std::move(result);
}

OdbcScanFunction::OdbcScanFunction()
    : TableFunction("odbc_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ScanOdbcTable, BindScan,
                   InitGlobalScanState, InitLocalScanState) {
    projection_pushdown = true;
    named_parameters["all_varchar"] = LogicalType(LogicalTypeId::BOOLEAN);
}

OdbcAttachFunction::OdbcAttachFunction()
    : TableFunction("odbc_attach", {LogicalType::VARCHAR}, AttachDatabase, BindAttach) {
    named_parameters["overwrite"] = LogicalType(LogicalTypeId::BOOLEAN);
}

OdbcQueryFunction::OdbcQueryFunction()
    : TableFunction("odbc_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ScanOdbcTable, BindQuery,
                   InitGlobalScanState, InitLocalScanState) {
    projection_pushdown = false;  // Can't push projection to arbitrary queries
    named_parameters["all_varchar"] = LogicalType(LogicalTypeId::BOOLEAN);
}

} // namespace duckdb