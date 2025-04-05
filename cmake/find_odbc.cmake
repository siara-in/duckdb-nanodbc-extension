# This module finds ODBC libraries and headers across all major platforms
# It sets the following variables:
#  ODBC_FOUND - True if ODBC was found
#  ODBC_INCLUDE_DIRS - ODBC include directories
#  ODBC_LIBRARIES - ODBC libraries to link against
#  ODBC_VERSION - ODBC version if available

# Try to use the standard CMake module first
find_package(ODBC QUIET)

# Set default return values
set(ODBC_FOUND FALSE)
set(ODBC_INCLUDE_DIRS "")
set(ODBC_LIBRARIES "")

# Define common search paths for each platform
if(WIN32)
  # Windows - Look for the SDK
  set(ODBC_INCLUDE_SEARCH_PATHS
    "C:/Program Files/Microsoft SDKs/Windows/v7.0/Include"
    "C:/Program Files/Microsoft SDKs/Windows/v6.0A/Include"
    "C:/Program Files (x86)/Microsoft SDKs/Windows/v7.0A/Include"
    "C:/Program Files/Microsoft SDKs/Windows/v7.1A/Include"
  )
  set(ODBC_LIBRARY_SEARCH_PATHS
    "C:/Program Files/Microsoft SDKs/Windows/v7.0/Lib"
    "C:/Program Files/Microsoft SDKs/Windows/v6.0A/Lib"
    "C:/Program Files (x86)/Microsoft SDKs/Windows/v7.0A/Lib"
    "C:/Program Files/Microsoft SDKs/Windows/v7.1A/Lib"
  )
elseif(APPLE)
  # macOS - Check both Intel and ARM paths
  set(ODBC_INCLUDE_SEARCH_PATHS
    "/usr/local/include"
    "/opt/homebrew/include"
    "/opt/local/include"
    "/usr/include"
    "/usr/include/iodbc"
    "/usr/local/include/iodbc"
    "/opt/homebrew/include/iodbc"
  )
  set(ODBC_LIBRARY_SEARCH_PATHS
    "/usr/local/lib"
    "/opt/homebrew/lib"
    "/opt/local/lib"
    "/usr/lib"
  ) 
else()
  # Linux/Unix
  set(ODBC_INCLUDE_SEARCH_PATHS
    "/usr/include"
    "/usr/include/odbc"
    "/usr/local/include"
    "/usr/local/include/odbc"
    "/usr/include/iodbc"
    "/usr/include/postgresql"
    "/usr/local/include/postgresql"
    "/usr/include/x86_64-linux-gnu"
    "/usr/include/x86_64-linux-gnu/odbc"
  )
  set(ODBC_LIBRARY_SEARCH_PATHS
    "/usr/lib"
    "/usr/lib/x86_64-linux-gnu"
    "/usr/lib/x86_64-linux-gnu/odbc"
    "/usr/local/lib"
    "/usr/lib/odbc"
    "/usr/lib64"
    "/usr/lib64/odbc"
  )
endif()

# Find header files
find_path(ODBC_INCLUDE_DIR
  NAMES sql.h sqlext.h sqltypes.h
  PATHS ${ODBC_INCLUDE_SEARCH_PATHS}
)

# Find libraries based on platform
if(WIN32)
  # Windows - odbc32.lib is the standard library
  find_library(ODBC_LIBRARY
    NAMES odbc32
    PATHS ${ODBC_LIBRARY_SEARCH_PATHS}
  )
  
  find_library(ODBCINST_LIBRARY
    NAMES odbccp32
    PATHS ${ODBC_LIBRARY_SEARCH_PATHS}
  )
  
  set(ODBC_LIBRARY_NAMES odbc32)
  set(ODBCINST_LIBRARY_NAMES odbccp32)
  
elseif(APPLE)
  # macOS - can use either unixODBC or iODBC
  find_library(ODBC_LIBRARY
    NAMES odbc iodbc
    PATHS ${ODBC_LIBRARY_SEARCH_PATHS}
  )
  
  find_library(ODBCINST_LIBRARY
    NAMES odbcinst iodbcinst
    PATHS ${ODBC_LIBRARY_SEARCH_PATHS}
  )
  
  find_library(ODBCCR_LIBRARY
    NAMES odbccr
    PATHS ${ODBC_LIBRARY_SEARCH_PATHS}
  )
  
  set(ODBC_LIBRARY_NAMES "odbc, iodbc")
  set(ODBCINST_LIBRARY_NAMES "odbcinst, iodbcinst")
  
else()
  # Linux/Unix - can use unixODBC or iODBC
  find_library(ODBC_LIBRARY
    NAMES odbc unixodbc iodbc
    PATHS ${ODBC_LIBRARY_SEARCH_PATHS}
  )
  
  find_library(ODBCINST_LIBRARY
    NAMES odbcinst iodbcinst
    PATHS ${ODBC_LIBRARY_SEARCH_PATHS}
  )
  
  find_library(ODBCCR_LIBRARY
    NAMES odbccr
    PATHS ${ODBC_LIBRARY_SEARCH_PATHS}
  )
  
  set(ODBC_LIBRARY_NAMES "odbc, unixodbc, iodbc")
  set(ODBCINST_LIBRARY_NAMES "odbcinst, iodbcinst")
endif()

# Set up the results
if(ODBC_INCLUDE_DIR AND ODBC_LIBRARY)
  set(ODBC_FOUND TRUE)
  
  # Set include dirs
  set(ODBC_INCLUDE_DIRS ${ODBC_INCLUDE_DIR})
  
  # Set libraries
  set(ODBC_LIBRARIES ${ODBC_LIBRARY})
  
  if(ODBCINST_LIBRARY)
    list(APPEND ODBC_LIBRARIES ${ODBCINST_LIBRARY})
  endif()
  
  if(ODBCCR_LIBRARY)
    list(APPEND ODBC_LIBRARIES ${ODBCCR_LIBRARY})
  endif()
  
  # Try to determine the version
  if(EXISTS "${ODBC_INCLUDE_DIR}/sqlext.h")
    file(STRINGS "${ODBC_INCLUDE_DIR}/sqlext.h" odbc_version_str
         REGEX "^#define[\t ]+ODBCVER[\t ]+0x[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F].*")
    
    if(odbc_version_str)
      string(REGEX REPLACE "^#define[\t ]+ODBCVER[\t ]+0x([0-9a-fA-F][0-9a-fA-F])([0-9a-fA-F][0-9a-fA-F]).*" "\\1.\\2" ODBC_VERSION "${odbc_version_str}")
      string(REGEX REPLACE "^([0-9])\\." "\\1" ODBC_VERSION_MAJOR "${ODBC_VERSION}")
      string(REGEX REPLACE "^[0-9]\\.([0-9][0-9])" "\\1" ODBC_VERSION_MINOR "${ODBC_VERSION}")
      set(ODBC_VERSION "${ODBC_VERSION_MAJOR}.${ODBC_VERSION_MINOR}")
    endif()
  endif()
  
  # Debug output
  if(NOT ODBC_FIND_QUIETLY)
    message(STATUS "Found ODBC: ${ODBC_LIBRARIES}")
    if(DEFINED ODBC_VERSION)
      message(STATUS "ODBC Version: ${ODBC_VERSION}")
    endif()
  endif()
  
else()
  # Could not find both required components
  if(NOT ODBC_INCLUDE_DIR)
    message(STATUS "ODBC headers not found. Required files: sql.h, sqlext.h, sqltypes.h")
    message(STATUS "Search paths were: ${ODBC_INCLUDE_SEARCH_PATHS}")
  endif()
  
  if(NOT ODBC_LIBRARY)
    message(STATUS "ODBC library not found. Looked for: ${ODBC_LIBRARY_NAMES}")
    message(STATUS "Search paths were: ${ODBC_LIBRARY_SEARCH_PATHS}")
  endif()
  
  if(ODBC_FIND_REQUIRED)
    message(FATAL_ERROR "Required ODBC package not found. Install ODBC driver manager:")
    if(WIN32)
      message(FATAL_ERROR "  Windows already includes ODBC. Make sure the SDK is installed.")
    elseif(APPLE)
      message(FATAL_ERROR "  macOS: Install with 'brew install unixodbc' or 'brew install libiodbc'")
    else()
      message(FATAL_ERROR "  Linux: Install with 'apt-get install unixodbc-dev' or equivalent")
    endif()
  endif()
endif()

# Handle QUIET and REQUIRED arguments
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ODBC
  REQUIRED_VARS ODBC_LIBRARIES ODBC_INCLUDE_DIRS
  VERSION_VAR ODBC_VERSION
)

# Mark as advanced
mark_as_advanced(ODBC_INCLUDE_DIR ODBC_LIBRARY ODBCINST_LIBRARY ODBCCR_LIBRARY)