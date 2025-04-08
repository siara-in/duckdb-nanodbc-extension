# DuckDB ODBC Extension

The DuckDB ODBC extension allows DuckDB to seamlessly connect to any database system that provides an ODBC driver. This enables you to query and analyze data from a wide variety of data sources without leaving the DuckDB ecosystem.

## Features

- **Simple ODBC Connectivity**: Connect to any ODBC-compatible database using DSNs or direct connection strings
- **Table Scanning**: Import tables from external databases into DuckDB for analysis
- **Cross-Database SQL**: Execute custom SQL queries directly against external databases
- **Automatic Attachment**: Attach all tables from an external database as views in DuckDB
- **Cross-Platform Support**: Works on Windows, macOS, and Linux
- **Type Handling**: Automatic mapping between ODBC and DuckDB data types
- **Performance Optimization**: Efficient data transfer with binary conversion

## Installation

```sql
INSTALL odbc FROM community;
LOAD odbc;
```

## Usage Examples

### Query a table using a pre-configured DSN

```sql
-- Basic usage with a DSN
SELECT * FROM odbc_scan('customers', 'MyODBCDSN');

-- With username and password
SELECT * FROM odbc_scan('orders', 'MyODBCDSN', 'username', 'password');
```

### Use a direct connection string

```sql
-- SQL Server example
SELECT * FROM odbc_scan('products', 'Driver={SQL Server};Server=myserver;Database=mydatabase;Trusted_Connection=yes;');

-- MySQL example
SELECT * FROM odbc_scan('customers', 'Driver={MySQL ODBC Driver};Server=localhost;Database=mydb;User=myuser;Password=mypassword;');
```

### Handle complex data types

```sql
-- Treat all columns as VARCHAR (useful for complex types)
SELECT * FROM odbc_scan('complex_table', 'MyODBCDSN', all_varchar=true);
```

### Execute custom SQL query

```sql
-- Execute a custom SQL query against an ODBC source
SELECT * FROM odbc_query('MyODBCDSN', 'SELECT id, name, amount FROM sales WHERE amount > 1000');
```

### Attach entire database

```sql
-- Attach all tables from an ODBC source as views in DuckDB
CALL odbc_attach('MyODBCDSN');

-- Attach with overwrite option (replace existing views)
CALL odbc_attach('MyODBCDSN', overwrite=true);
```

## Compatibility Matrix

This extension has been tested with various database systems across different platforms. Below is a matrix showing confirmed compatibility:

| Database System | Windows | macOS | Linux |
|-----------------|:-------:|:-----:|:-----:|
| SQL Server | ✅ | ✅ | ✅ |
| Microsoft Access | ✅ | - | - |
| MySQL | ✅ | ✅ | ✅ |
| PostgreSQL | ✅ | ✅ | ✅ |

_Note: Compatibility depends on the availability of ODBC drivers for your platform. The matrix above shows databases I've verified, but any database with proper ODBC drivers should work._

## Requirements

### Windows
- Windows ODBC drivers are included in the operating system
- Additional database-specific drivers may need to be installed separately

### macOS
- Install unixODBC: `brew install unixodbc`
- Install database-specific drivers

### Linux
- Install unixODBC: `apt-get install unixodbc unixodbc-dev` (Ubuntu/Debian) or `yum install unixODBC unixODBC-devel` (RHEL/CentOS)
- Install database-specific drivers

## Configuration

### Setting up DSNs

#### Windows
1. Open "ODBC Data Sources" from Administrative Tools
2. Add a new User or System DSN with your connection details

#### macOS/Linux
1. Edit `~/.odbc.ini` to add your DSN:
```ini
[MyODBCDSN]
Driver = PostgreSQL
Server = localhost
Database = mydb
```
2. Edit `~/.odbcinst.ini` to configure drivers:
```ini
[PostgreSQL]
Description = PostgreSQL ODBC Driver
Driver = /usr/local/lib/psqlodbcw.so
```

## Troubleshooting

- **Connection Errors**: Ensure your DSN is properly configured and the database server is accessible
- **Missing Drivers**: Verify that the appropriate ODBC drivers are installed for your database system
- **Type Conversion Issues**: Use the `all_varchar=true` option if you encounter type conversion problems

## Performance Considerations

- For large datasets, consider using `LIMIT` or filtering conditions in your queries
- The extension performs best when retrieving specific columns rather than `SELECT *`
- Complex joins are better performed within DuckDB after importing the necessary tables

## License

This extension is licensed under the MIT License.