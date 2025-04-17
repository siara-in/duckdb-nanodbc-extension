cmake_minimum_required(VERSION 3.5)
set(CMAKE_CXX_STANDARD 17)

# Set extension name here
set(TARGET_NAME odbc)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Include directories
include_directories(src/include)
include_directories(src)

# Platform-specific handling for ODBC
if(WIN32)
    # Windows ODBC is included in the SDK
    set(ODBC_LIBRARIES odbc32)
    set(ODBC_FOUND TRUE)
    set(ODBC_INCLUDE_DIRS "")
    message(STATUS "Using Windows SDK ODBC libraries")
else()
    # For macOS and Linux, use vcpkg-provided unixODBC
    find_package(ODBC REQUIRED)
    
    if(NOT ODBC_FOUND)
        message(FATAL_ERROR "ODBC not found. Please ensure your vcpkg.json includes unixodbc as a dependency.")
    endif()
    
    message(STATUS "Found ODBC libraries: ${ODBC_LIBRARIES}")
    message(STATUS "Found ODBC includes: ${ODBC_INCLUDE_DIRS}")
    
    # On Linux, make sure we're using the right linking flags
    if(UNIX AND NOT APPLE)
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-as-needed")
    endif()
endif()

# Include ODBC headers
include_directories(${ODBC_INCLUDE_DIRS})

# Nanodbc library files
set(NANODBC_SOURCES
    src/nanodbc/nanodbc.cpp
)

# Extension sources
set(EXTENSION_SOURCES 
    src/nanodbc_db.cpp
    src/nanodbc_stmt.cpp
    src/nanodbc_utils.cpp
    src/nanodbc_scanner.cpp
    src/nanodbc_extension.cpp
    src/odbc_exports.cpp)

# Combined sources
set(ALL_SOURCES ${EXTENSION_SOURCES} ${NANODBC_SOURCES})

# Create the static extension
add_library(${EXTENSION_NAME} STATIC ${ALL_SOURCES})
target_link_libraries(${EXTENSION_NAME} PRIVATE ${ODBC_LIBRARIES})

# Build loadable extension with ODBC parameter
# Use the actual path to the ODBC library instead of -lodbc
set(PARAMETERS "-l${ODBC_LIBRARIES}")
build_loadable_extension(${TARGET_NAME} "${PARAMETERS}" ${ALL_SOURCES})

# Install the extension
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")