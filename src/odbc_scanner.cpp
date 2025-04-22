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
        // Validate inputs
        if (input.inputs.size() < 1) {
            throw BinderException("ODBC attach requires a DSN or connection string");
        }
        
        if (input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
            throw BinderException("First argument to ODBC attach must be VARCHAR (connection info)");
        }
        
        auto result = make_uniq<OdbcAttachFunctionData>();
        
        // Parse connection info and optional credentials
        std::string connection_info = input.inputs[0].GetValue<string>();
        std::string username = input.inputs.size() >= 2 ? input.inputs[1].GetValue<string>() : "";
        std::string password = input.inputs.size() >= 3 ? input.inputs[2].GetValue<string>() : "";
        
        result->connection_params = ConnectionParams(connection_info, username, password);
        
        // Process options from named parameters
        for (auto &kv : input.named_parameters) {
            if (kv.first == "all_varchar") {
                result->all_varchar = BooleanValue::Get(kv.second);
            } else if (kv.first == "overwrite") {
                result->overwrite = BooleanValue::Get(kv.second);
            }
        }
        
        // Set up return types (single boolean column)
        return_types.emplace_back(LogicalTypeId::BOOLEAN);
        names.emplace_back("Success");
        
        return std::move(result);
    };
    
    // Add named parameters
    result.named_parameters["all_varchar"] = LogicalTypeId::BOOLEAN;
    result.named_parameters["overwrite"] = LogicalTypeId::BOOLEAN;
    
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
                try {
                    // Connect to data source
                    auto db = OdbcConnection::Connect(result->connection_params);
                    
                    // For QUERY, prepare statement to get schema
                    auto stmt = db->Prepare(result->sql);
                    
                    // Get column information
                    auto columnCount = stmt->GetColumnCount();
                    
                    // If no columns are returned (likely a DDL statement), 
                    // add a default "Success" column
                    if (columnCount == 0) {
                        names.push_back("Success");
                        return_types.push_back(LogicalType(LogicalTypeId::BOOLEAN));
                    } else {
                        // Original code for handling result columns
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
        
        // Special handling for DDL statements (indicated by a single "Success" column)
        if (bind_data.column_names.size() == 1 && bind_data.column_names[0] == "Success") {
            // This is a DDL statement, execute it directly without preparing
            try {
                // Use the connection's Execute method for DDL statements
                result->connection->Execute(bind_data.sql);
                result->done = false; // Will return a single success row
            } catch (const nanodbc::database_error& e) {
                OdbcUtils::ThrowException("execute statement", e);
            }
            return std::move(result);
        }
        
        // Prepare the query (original code for regular queries)
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
    auto &bind_data = data.bind_data->Cast<OdbcScannerState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }

    // If there's only one column and it's named "Success" (our indicator for DDL statements)
    if (output.ColumnCount() == 1 && 
        bind_data.column_names.size() == 1 && 
        bind_data.column_names[0] == "Success") {
        
        // For DDL statements, we just execute the statement without fetching results
        try {
            // The statement has already been prepared and executed in InitOdbcLocalState
            // Just mark as done and set success value
            if (output.data[0].GetType().id() == LogicalTypeId::BOOLEAN) {
                FlatVector::GetData<bool>(output.data[0])[0] = true;
            }
            
            output.SetCardinality(1);
            state.done = true;
            return;
        } catch (...) {
            // If an error occurred during execution, it would have been thrown earlier
            // This is just a safeguard
            output.SetCardinality(0);
            state.done = true;
            return;
        }
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
    auto &attach_data = data.bind_data->CastNoConst<OdbcAttachFunctionData>();
    
    // If we've already finished, just return
    if (attach_data.finished) {
        output.SetCardinality(0);
        return;
    }
    
    try {
        // Connect to the ODBC data source
        auto db = OdbcConnection::Connect(attach_data.connection_params);
        
        // Create connection to DuckDB
        auto dconn = Connection(context.db->GetDatabase(context));
        
        // Step 1: Handle tables - create views for each table
        auto tables = db->GetTables();
        for (auto &table_name : tables) {
            // Build arguments list
            std::vector<Value> args;
            args.push_back(Value(table_name));
            
            if (attach_data.connection_params.GetDsn().empty()) {
                // Connection string approach
                args.push_back(Value(attach_data.connection_params.GetConnectionString()));
            } else {
                // DSN approach
                args.push_back(Value(attach_data.connection_params.GetDsn()));
                
                if (!attach_data.connection_params.GetUsername().empty()) {
                    args.push_back(Value(attach_data.connection_params.GetUsername()));
                    
                    if (!attach_data.connection_params.GetPassword().empty()) {
                        args.push_back(Value(attach_data.connection_params.GetPassword()));
                    }
                }
            }
            
            // Add all_varchar parameter if specified
            duckdb::vector<Value> duckdb_args;
            for (const auto& arg : args) {
                duckdb_args.push_back(arg);
            }
            
            duckdb::named_parameter_map_t named_params;
            if (attach_data.all_varchar) {
                named_params["all_varchar"] = Value::BOOLEAN(true);
            }
            
            auto table_func_relation = dconn.TableFunction("odbc_scan", duckdb_args, named_params);
            table_func_relation->CreateView(table_name, attach_data.overwrite, false);
        }
        
        // Step 2: Handle views
        auto views = db->GetViews();
        for (auto &view_name : views) {
            // For views, we use odbc_query with SELECT * FROM view
            std::vector<Value> query_args;
            
            if (attach_data.connection_params.GetDsn().empty()) {
                // Connection string approach
                query_args.push_back(Value(attach_data.connection_params.GetConnectionString()));
            } else {
                // DSN approach
                query_args.push_back(Value(attach_data.connection_params.GetDsn()));
                
                if (!attach_data.connection_params.GetUsername().empty()) {
                    query_args.push_back(Value(attach_data.connection_params.GetUsername()));
                    
                    if (!attach_data.connection_params.GetPassword().empty()) {
                        query_args.push_back(Value(attach_data.connection_params.GetPassword()));
                    }
                }
            }
            
            // The SQL will select from the view
            query_args.push_back(Value("SELECT * FROM \"" + OdbcUtils::SanitizeString(view_name) + "\""));
            
            duckdb::vector<Value> duckdb_args;
            for (const auto& arg : query_args) {
                duckdb_args.push_back(arg);
            }
            
            // Create a DuckDB view based on the query
            duckdb::named_parameter_map_t named_params;
            if (attach_data.all_varchar) {
                named_params["all_varchar"] = Value::BOOLEAN(true);
            }
            
            auto query_func_relation = dconn.TableFunction("odbc_query", duckdb_args, named_params);
            query_func_relation->CreateView(view_name, attach_data.overwrite, false);
        }
        
        // Set output
        output.SetCardinality(1);
        output.SetValue(0, 0, Value::BOOLEAN(true));
        
        // Mark as finished so we don't execute again
        attach_data.finished = true;
        
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("attach database", e);
    }
}


TableFunction OdbcTableFunction::CreateExecFunction() {
    TableFunction result("odbc_exec", {LogicalType::VARCHAR}, ExecuteOdbcStatement);
    
    // Setup binding function
    result.bind = [](ClientContext &context, TableFunctionBindInput &input,
                     vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData> {
        // Validate inputs
        if (input.inputs.size() < 1) {
            throw BinderException("ODBC exec requires a DSN or connection string");
        }
        
        if (input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
            throw BinderException("First argument to ODBC exec must be VARCHAR (connection info)");
        }
        
        auto result = make_uniq<OdbcExecFunctionData>();
        
        // Parse connection info and optional credentials
        std::string connection_info = input.inputs[0].GetValue<string>();
        std::string username = input.inputs.size() >= 2 ? input.inputs[1].GetValue<string>() : "";
        std::string password = input.inputs.size() >= 3 ? input.inputs[2].GetValue<string>() : "";
        
        result->connection_params = ConnectionParams(connection_info, username, password);
        
        // Get SQL from named parameter 'sql' (required)
        auto sql_param = input.named_parameters.find("sql");
        if (sql_param == input.named_parameters.end()) {
            throw BinderException("ODBC exec requires 'sql' parameter");
        }
        result->sql = sql_param->second.ToString();
        
        // Set up return types (single boolean column)
        return_types.emplace_back(LogicalTypeId::BOOLEAN);
        names.emplace_back("Success");
        
        return std::move(result);
    };
    
    // Add required sql parameter
    result.named_parameters["sql"] = LogicalType(LogicalTypeId::VARCHAR);
    
    return result;
}

TableFunction OdbcExecFunction() {
    return OdbcTableFunction::CreateExecFunction();
}

// Function to execute the statement
void ExecuteOdbcStatement(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &exec_data = data.bind_data->CastNoConst<OdbcExecFunctionData>();
    
    // If we've already finished, just return
    if (exec_data.finished) {
        output.SetCardinality(0);
        return;
    }
    
    try {
        // Connect to data source
        auto db = OdbcConnection::Connect(exec_data.connection_params);
        
        // Execute the SQL statement directly
        db->Execute(exec_data.sql);
        
        // Set success output
        output.SetCardinality(1);
        auto &success_vector = output.data[0];
        FlatVector::GetData<bool>(success_vector)[0] = true;
        
        // Mark as finished so we don't execute again
        exec_data.finished = true;
        
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("execute statement", e);
    }
}
} // namespace duckdb