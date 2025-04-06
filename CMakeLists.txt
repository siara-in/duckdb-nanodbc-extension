cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME odbc)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Include directories
include_directories(src/include)

# Platform-specific handling for ODBC
if(WIN32)
  # Windows ODBC is included in the SDK
  set(ODBC_LIBRARIES odbc32)
  set(ODBC_FOUND TRUE)
  set(ODBC_INCLUDE_DIRS "")
  message(STATUS "Using Windows SDK ODBC libraries")
else()
  # For macOS and Linux, use vcpkg-provided unixODBC
  # When using vcpkg, find_package should automatically find the installed package
  find_package(ODBC REQUIRED)
  
  if(NOT ODBC_FOUND)
    message(FATAL_ERROR 
      "ODBC not found. Please ensure your vcpkg.json includes unixodbc as a dependency.\n"
      "If using a manual build, install ODBC development libraries:\n"
      "  macOS: brew install unixodbc\n"
      "  Debian/Ubuntu: sudo apt-get install unixodbc-dev\n"
      "  RHEL/CentOS: sudo yum install unixODBC-devel")
  endif()
  
  message(STATUS "Found ODBC libraries: ${ODBC_LIBRARIES}")
  message(STATUS "Found ODBC includes: ${ODBC_INCLUDE_DIRS}")
endif()

# Include ODBC headers
include_directories(${ODBC_INCLUDE_DIRS})

# Extension sources
set(EXTENSION_SOURCES 
    src/odbc_db.cpp
    src/odbc_stmt.cpp
    src/odbc_scanner.cpp
    src/odbc_utils.cpp
    src/odbc_extension.cpp)

# Build the extension
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link against ODBC libraries for all targets
target_link_libraries(${EXTENSION_NAME} ${ODBC_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${ODBC_LIBRARIES})

# Install the extension
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")