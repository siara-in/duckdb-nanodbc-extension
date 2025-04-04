cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME odbc)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Find ODBC
if(WIN32)
  # Windows ODBC is included in the SDK
  set(ODBC_LIBRARIES odbc32)
else()
  find_package(ODBC REQUIRED)
  include_directories(${ODBC_INCLUDE_DIRS})
  
  # Some systems require explicitly finding the odbc library
  find_library(ODBC_LIBRARY NAMES odbc)
  if(NOT ODBC_LIBRARY)
    message(FATAL_ERROR "ODBC library not found. Install unixodbc-dev (Debian/Ubuntu) or equivalent.")
  endif()
  
  # Add both ODBC_LIBRARIES and ODBC_LIBRARY to ensure comprehensive linkage
  set(ODBC_LIBRARIES ${ODBC_LIBRARIES} ${ODBC_LIBRARY})
  message(STATUS "Found ODBC libraries: ${ODBC_LIBRARIES}")
endif()

set(EXTENSION_SOURCES 
    src/odbc_db.cpp
    src/odbc_stmt.cpp
    src/odbc_scanner.cpp
    src/odbc_utils.cpp
    src/odbc_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})


install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
