cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME odbc)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Add our custom modules directory to the module path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Include directories
include_directories(src/include)

# Find ODBC using our enhanced module
find_package(ODBC REQUIRED)
include_directories(${ODBC_INCLUDE_DIRS})

message(STATUS "ODBC_LIBRARIES: ${ODBC_LIBRARIES}")
message(STATUS "ODBC_INCLUDE_DIRS: ${ODBC_INCLUDE_DIRS}")
if(DEFINED ODBC_VERSION)
  message(STATUS "ODBC Version: ${ODBC_VERSION}")
endif()

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