#pragma once

// This header ensures proper ODBC header inclusion across platforms

#ifdef _WIN32
    // Windows-specific includes
    #include <windows.h>

    // Ensure ODBCVER is defined before including ODBC headers
    #ifndef ODBCVER
        #define ODBCVER 0x0380
    #endif

    // Make sure SQLLEN is properly defined
    #ifdef _WIN64
        #ifndef SQLLEN
            typedef INT64 SQLLEN;
        #endif
        #ifndef SQLULEN
            typedef UINT64 SQLULEN;
        #endif
        #ifndef SQLSETPOSIROW
            typedef UINT64 SQLSETPOSIROW;
        #endif
    #else
        #ifndef SQLLEN
            typedef SQLINTEGER SQLLEN;
        #endif
        #ifndef SQLULEN
            typedef SQLUINTEGER SQLULEN;
        #endif
        #ifndef SQLSETPOSIROW
            typedef SQLUSMALLINT SQLSETPOSIROW;
        #endif
    #endif

    // Include ODBC headers in the correct order
    #include <sql.h>
    #include <sqlext.h>
    #include <sqltypes.h>
    #include <sqlucode.h>
#else
    // Unix/macOS includes
    #include <sql.h>
    #include <sqlext.h>
    #include <sqltypes.h>
    #include <sqlucode.h>
#endif