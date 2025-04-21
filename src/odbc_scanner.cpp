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

//------------------------------------------------------------------------------
// OdbcTableFunction implementations
//------------------------------------------------------------------------------

TableFunction OdbcTableFunction::CreateScanFunction() {
    TableFunction result("odbc_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ScanOdbcSource);
    
    // Setup binding function
    result.bind = [](ClientContext &context, TableFunctionBindInput &input,
                     vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData> {
        return BindOdbcFunction(context, input, return_types, names, OdbcOperation::SCAN);
    };
    
    // Setup state initialization
    result.init_global = InitOdbcGlobalState;
    result.init_local = InitOdbcLocalState;
    result.projection_pushdown = true;
    
    // Add named parameters
    result.named_parameters["all_varchar"] = LogicalType(LogicalTypeId::BOOLEAN);
    
    return result;
}

TableFunction OdbcTableFunction::CreateAttachFunction() {
    TableFunction result("odbc_attach", {LogicalType::VARCHAR}, AttachOdbcDatabase);
    
    // Setup binding function
    result.bind = [](ClientContext &context, TableFunctionBindInput &input,
                     vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData> {
        return BindOdbcFunction(context, input, return_types, names, OdbcOperation::ATTACH);
    };
    
    // Add named parameters
    result.named_parameters["all_varchar"] = LogicalType(LogicalTypeId::BOOLEAN);
    result.named_parameters["overwrite"] = LogicalType(LogicalTypeId::BOOLEAN);
    
    return result;
}

TableFunction OdbcTableFunction::CreateQueryFunction() {
    TableFunction result("odbc_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ScanOdbcSource);
    
    // Setup binding function
    result.bind = [](ClientContext &context, TableFunctionBindInput &input,
                     vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData> {
        return BindOdbcFunction(context, input, return_types, names, OdbcOperation::QUERY);
    };
    
    // Setup state initialization
    result.init_global = InitOdbcGlobalState;
    result.init_local = InitOdbcLocalState;
    result.projection_pushdown = false;
    
    // Add named parameters
    result.named_parameters["all_varchar"] = LogicalType(LogicalTypeId::BOOLEAN);
    
    return result;
}

TableFunction OdbcScanFunction() {
    return OdbcTableFunction::CreateScanFunction();
}

TableFunction OdbcAttachFunction() {
    return OdbcTableFunction::CreateAttachFunction();
}

TableFunction OdbcQueryFunction() {
    return OdbcTableFunction::CreateQueryFunction();
}

//------------------------------------------------------------------------------
// Binding Functions
//------------------------------------------------------------------------------

unique_ptr<FunctionData> BindOdbcFunction(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names,
                                        OdbcOperation operation) {
    auto result = make_uniq<OdbcScannerState>();
    
    // Process connection information based on operation
    switch (operation) {
        case OdbcOperation::SCAN: {
            // Validate inputs
            if (input.inputs.size() < 2) {
                throw BinderException("ODBC scan requires at least a table name and either a DSN or connection string");
            }
            
            if (input.inputs[0].type().id() != LogicalTypeId::VARCHAR ||
                input.inputs[1].type().id() != LogicalTypeId::VARCHAR) {
                throw BinderException("First two arguments to ODBC scan must be VARCHAR (table name and connection info)");
            }
            
            // Get table name and connection info
            result->table_name = input.inputs[0].GetValue<string>();
            
            // Parse connection info and optional credentials
            std::string connection_info = input.inputs[1].GetValue<string>();
            std::string username = input.inputs.size() >= 3 ? input.inputs[2].GetValue<string>() : "";
            std::string password = input.inputs.size() >= 4 ? input.inputs[3].GetValue<string>() : "";
            
            result->connection_params = ConnectionParams(connection_info, username, password);
            break;
        }
        
        case OdbcOperation::ATTACH: {
            // Validate inputs
            if (input.inputs.size() < 1) {
                throw BinderException("ODBC attach requires a DSN or connection string");
            }
            
            if (input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
                throw BinderException("First argument to ODBC attach must be VARCHAR (connection info)");
            }
            
            // Parse connection info and optional credentials
            std::string connection_info = input.inputs[0].GetValue<string>();
            std::string username = input.inputs.size() >= 2 ? input.inputs[1].GetValue<string>() : "";
            std::string password = input.inputs.size() >= 3 ? input.inputs[2].GetValue<string>() : "";
            
            result->connection_params = ConnectionParams(connection_info, username, password);
            
            // Return types for ATTACH
            return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
            names.emplace_back("Success");
            break;
        }
        
        case OdbcOperation::QUERY: {
            // Validate inputs
            if (input.inputs.size() < 2) {
                throw BinderException("ODBC query requires a connection string and SQL query");
            }
            
            if (input.inputs[0].type().id() != LogicalTypeId::VARCHAR ||
                input.inputs[1].type().id() != LogicalTypeId::VARCHAR) {
                throw BinderException("First two arguments to ODBC query must be VARCHAR (connection info and SQL query)");
            }
            
            // Get connection info and SQL
            std::string connection_info = input.inputs[0].GetValue<string>();
            result->sql = input.inputs[1].GetValue<string>();
            
            // Parse credentials if provided
            std::string username = input.inputs.size() >= 3 ? input.inputs[2].GetValue<string>() : "";
            std::string password = input.inputs.size() >= 4 ? input.inputs[3].GetValue<string>() : "";
            
            result->connection_params = ConnectionParams(connection_info, username, password);
            break;
        }
    }
    
    // Process options from named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "all_varchar") {
            result->all_varchar = BooleanValue::Get(kv.second);
        }
        
        // Store all parameters for later use
        result->named_parameters[kv.first] = kv.second;
    }
    
    // For SCAN and QUERY, we need to fetch schema information
    if (operation == OdbcOperation::SCAN || operation == OdbcOperation::QUERY) {
        try {
            // Connect to data source
            auto db = OdbcConnection::Connect(result->connection_params);
            
            if (operation == OdbcOperation::SCAN) {
                // Get table information from database
                ColumnList columns;
                std::vector<std::unique_ptr<Constraint>> constraints;
                db->GetTableInfo(result->table_name, columns, constraints, result->all_varchar);
                
                // Map column types and names
                for (auto &column : columns.Logical()) {
                    names.push_back(column.GetName());
                    return_types.push_back(column.GetType());
                }
                
                if (names.empty()) {
                    throw BinderException("No columns found for table " + result->table_name);
                }
            } else {
                // For QUERY, prepare statement to get schema
                auto stmt = db->Prepare(result->sql);
                
                // Get column information
                auto columnCount = stmt->GetColumnCount();
                
                for (idx_t i = 0; i < columnCount; i++) {
                    auto colName = stmt->GetName(i);
                    SQLULEN size = 0;
                    SQLSMALLINT digits = 0;
                    SQLSMALLINT odbcType = stmt->GetOdbcType(i, &size, &digits);
                    
                    auto duckType = result->all_varchar ? 
                                  LogicalType::VARCHAR : 
                                  OdbcUtils::OdbcTypeToLogicalType(odbcType, size, digits);
                    
                    names.push_back(colName);
                    return_types.push_back(duckType);
                }
            }
            
            // Store column information
            result->column_names = names;
            result->column_types = return_types;
            
        } catch (const nanodbc::database_error& e) {
            OdbcUtils::ThrowException("bind ODBC function", e);
        }
    }
    
    return std::move(result);
}

//------------------------------------------------------------------------------
// State Initialization
//------------------------------------------------------------------------------

unique_ptr<GlobalTableFunctionState> InitOdbcGlobalState(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<OdbcGlobalScanState>(1); // Single-threaded scan for now
}

unique_ptr<LocalTableFunctionState> InitOdbcLocalState(ExecutionContext &context, TableFunctionInitInput &input, 
                                                     GlobalTableFunctionState *global_state) {
    auto &bind_data = input.bind_data->Cast<OdbcScannerState>();
    auto result = make_uniq<OdbcLocalScanState>();
    
    // Store column IDs from input
    result->column_ids = input.column_ids;
    
    try {
        // Create connection
        result->connection = OdbcConnection::Connect(bind_data.connection_params);
        
        // Prepare the query
        std::string sql;
        if (bind_data.sql.empty()) {
            // Build query based on column IDs
            auto colNames = StringUtil::Join(
                result->column_ids.data(), result->column_ids.size(), ", ", [&](const idx_t columnId) {
                    return columnId == (column_t)-1 ? "NULL"
                                                  : '"' + OdbcUtils::SanitizeString(bind_data.column_names[columnId]) + '"';
                });
                
            sql = StringUtil::Format("SELECT %s FROM \"%s\"", colNames, 
                                   OdbcUtils::SanitizeString(bind_data.table_name));
        } else {
            sql = bind_data.sql;
        }
        
        // Prepare the statement
        result->statement = result->connection->Prepare(sql);
        result->done = false;
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("initialize scanner", e);
    }
    
    return std::move(result);
}

//------------------------------------------------------------------------------
// Scan Function
//------------------------------------------------------------------------------

void ScanOdbcSource(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.local_state->Cast<OdbcLocalScanState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    
    // Fetch rows and populate the DataChunk
    idx_t out_idx = 0;
    while (out_idx < STANDARD_VECTOR_SIZE) {
        if (!state.statement->Step()) {
            // No more rows to fetch
            state.done = true;
            break;
        }
        state.scan_count++;
        
        // Process each column
        for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
            auto &out_vec = output.data[col_idx];
            
            // Check for NULL
            if (state.statement->IsNull(col_idx)) {
                FlatVector::Validity(out_vec).Set(out_idx, false);
                continue;
            }
            
            // Based on the output vector type, convert and fetch the data
            switch (out_vec.GetType().id()) {
                case LogicalTypeId::BOOLEAN: {
                    FlatVector::GetData<bool>(out_vec)[out_idx] = (state.statement->GetInt32(col_idx) != 0);
                    break;
                }
                case LogicalTypeId::TINYINT: {
                    FlatVector::GetData<int8_t>(out_vec)[out_idx] = static_cast<int8_t>(state.statement->GetInt32(col_idx));
                    break;
                }
                case LogicalTypeId::SMALLINT: {
                    FlatVector::GetData<int16_t>(out_vec)[out_idx] = static_cast<int16_t>(state.statement->GetInt32(col_idx));
                    break;
                }
                case LogicalTypeId::INTEGER: {
                    FlatVector::GetData<int32_t>(out_vec)[out_idx] = state.statement->GetInt32(col_idx);
                    break;
                }
                case LogicalTypeId::BIGINT: {
                    FlatVector::GetData<int64_t>(out_vec)[out_idx] = state.statement->GetInt64(col_idx);
                    break;
                }
                case LogicalTypeId::FLOAT: {
                    FlatVector::GetData<float>(out_vec)[out_idx] = static_cast<float>(state.statement->GetDouble(col_idx));
                    break;
                }
                case LogicalTypeId::DOUBLE: {
                    FlatVector::GetData<double>(out_vec)[out_idx] = state.statement->GetDouble(col_idx);
                    break;
                }
                case LogicalTypeId::VARCHAR: {
                    std::string str_val = state.statement->GetString(col_idx);
                    FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                        StringVector::AddString(out_vec, str_val);
                    break;
                }
                case LogicalTypeId::DATE: {
                    // Extract date part from timestamp
                    timestamp_t ts = state.statement->GetTimestamp(col_idx);
                    FlatVector::GetData<date_t>(out_vec)[out_idx] = Timestamp::GetDate(ts);
                    break;
                }
                case LogicalTypeId::TIME: {
                    // Extract time part from timestamp
                    timestamp_t ts = state.statement->GetTimestamp(col_idx);
                    FlatVector::GetData<dtime_t>(out_vec)[out_idx] = Timestamp::GetTime(ts);
                    break;
                }
                case LogicalTypeId::TIMESTAMP: {
                    FlatVector::GetData<timestamp_t>(out_vec)[out_idx] = state.statement->GetTimestamp(col_idx);
                    break;
                }
                case LogicalTypeId::UUID: {
                    // UUIDs are typically handled as strings in ODBC
                    std::string uuidStr = state.statement->GetString(col_idx);
                    
                    // Convert string UUID to DuckDB UUID format
                    try {
                        hugeint_t uuidValue;
                        if (UUID::FromString(uuidStr, uuidValue)) {
                            FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = uuidValue;
                        } else {
                            // If parsing fails, set to NULL
                            FlatVector::Validity(out_vec).Set(out_idx, false);
                        }
                    } catch (...) {
                        // If any error occurs during conversion, set to NULL
                        FlatVector::Validity(out_vec).Set(out_idx, false);
                    }
                    break;
                }
                case LogicalTypeId::BLOB: {
                    std::string blob_data = state.statement->GetString(col_idx);
                    FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                        StringVector::AddStringOrBlob(out_vec, blob_data.data(), blob_data.size());
                    break;
                }
                default:
                    throw BinderException("Unsupported ODBC to DuckDB type conversion: " + 
                                       out_vec.GetType().ToString());
            }
        }
        
        out_idx++;
    }
    
    // Set the cardinality of the output chunk
    output.SetCardinality(out_idx);
}

//------------------------------------------------------------------------------
// Attach Function
//------------------------------------------------------------------------------

void AttachOdbcDatabase(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.bind_data->Cast<OdbcScannerState>();
    
    try {
        // Connect to the ODBC data source
        auto db = OdbcConnection::Connect(state.connection_params);
        
        // Get list of tables
        auto tables = db->GetTables();
        
        // Create connection to DuckDB
        auto dconn = Connection(context.db->GetDatabase(context));
        
        // Process additional parameters
        bool overwrite = false;
        for (auto &kv : state.named_parameters) {
            if (kv.first == "overwrite") {
                overwrite = BooleanValue::Get(kv.second);
            }
        }
        
        // Create views for each table
        for (auto &table_name : tables) {
            // Build arguments list
            std::vector<Value> args;
            args.push_back(Value(table_name));
            
            if (state.connection_params.GetDsn().empty()) {
                // Connection string approach
                args.push_back(Value(state.connection_params.GetConnectionString()));
            } else {
                // DSN approach
                args.push_back(Value(state.connection_params.GetDsn()));
                
                if (!state.connection_params.GetUsername().empty()) {
                    args.push_back(Value(state.connection_params.GetUsername()));
                    
                    if (!state.connection_params.GetPassword().empty()) {
                        args.push_back(Value(state.connection_params.GetPassword()));
                    }
                }
            }
            
            // Create view with vector type conversion
            duckdb::vector<Value> duckdb_args;
            for (const auto& arg : args) {
                duckdb_args.push_back(arg);
            }
            auto table_func_relation = dconn.TableFunction("odbc_scan", duckdb_args);
            table_func_relation->CreateView(table_name, overwrite, false);
        }
        
        // Set output
        output.SetCardinality(1);
        output.SetValue(0, 0, Value::BOOLEAN(true));
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("attach database", e);
    }
}

} // namespace duckdb