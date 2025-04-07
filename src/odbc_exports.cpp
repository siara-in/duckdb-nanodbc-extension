#include "duckdb.hpp"
#include "odbc_headers.hpp"

// Define the required symbols that seem to be missing
// These would normally be exported by the ODBC driver manager
#if defined(_WIN32)
  #define ODBC_EXPORT __declspec(dllexport)
#else
  #define ODBC_EXPORT __attribute__((visibility("default")))
#endif

extern "C" {
    // Define the odbc_version symbol - ODBC version 3.80
    ODBC_EXPORT int odbc_version = 0x0380;
    
    // Define a dummy odbc_init function
    ODBC_EXPORT void odbc_init(void) {
        // This function is typically called during ODBC initialization
        // For our purposes, an empty implementation should be sufficient
    }
}

namespace duckdb {
    // This file is just for exporting required symbols
}