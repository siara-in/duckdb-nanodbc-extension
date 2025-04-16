cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME odbc)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Include directories
include_directories(src/include)

# Download and configure nanodbc
include(FetchContent)
set(NANODBC_DISABLE_TESTS ON CACHE BOOL "Disable nanodbc tests")
set(NANODBC_DISABLE_EXAMPLES ON CACHE BOOL "Disable nanodbc examples")
set(NANODBC_DISABLE_INSTALL ON CACHE BOOL "Disable nanodbc installation")
FetchContent_Declare(
    nanodbc
    GIT_REPOSITORY https://github.com/nanodbc/nanodbc.git
    GIT_TAG v2.14.0
)
FetchContent_MakeAvailable(nanodbc)

# Set global visibility settings to ensure symbols are exported correctly
set(CMAKE_CXX_VISIBILITY_PRESET default)
set(CMAKE_VISIBILITY_INLINES_HIDDEN OFF)

# Add explicit export definitions for odbc_init and odbc_version
add_definitions(-DODBC_INIT_EXPORT)

# macOS-specific settings
if(APPLE)
    # Support for Apple Silicon (ARM64)
    set(CMAKE_OSX_ARCHITECTURES "arm64" CACHE STRING "Build architectures for macOS" FORCE)

    # Find iconv on macOS (it's part of the system but needs explicit linking)
    find_library(ICONV_LIBRARY NAMES iconv libiconv REQUIRED)
    if(ICONV_LIBRARY)
        message(STATUS "Found iconv library: ${ICONV_LIBRARY}")
    else()
        # On macOS, sometimes we need to add special handling for iconv
        set(ICONV_LIBRARY "iconv")
        message(STATUS "Using default iconv library")
    endif()
    
    # Set RPATH for macOS to make sure libraries are found at runtime
    set(CMAKE_INSTALL_RPATH "@loader_path")
    set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
    set(CMAKE_MACOSX_RPATH ON)
endif()

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
    
    # Add iconv to ODBC libraries on macOS
    if(APPLE AND ICONV_LIBRARY)
        list(APPEND ODBC_LIBRARIES ${ICONV_LIBRARY})
        message(STATUS "Added iconv to ODBC libraries: ${ODBC_LIBRARIES}")
    endif()
    
    # On Linux, make sure we're using the right linking flags
    if(UNIX AND NOT APPLE)
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-as-needed")
    endif()
endif()

# Include ODBC headers
include_directories(${ODBC_INCLUDE_DIRS})
include_directories(${nanodbc_SOURCE_DIR})

# Print information about the build environment
message(STATUS "Building for platform: ${CMAKE_SYSTEM_NAME}")
message(STATUS "Processor architecture: ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "CMake generator: ${CMAKE_GENERATOR}")
message(STATUS "Using nanodbc from: ${nanodbc_SOURCE_DIR}")

# Extension sources
set(EXTENSION_SOURCES 
    src/nanodbc_db.cpp
    src/nanodbc_stmt.cpp
    src/nanodbc_utils.cpp
    src/nanodbc_scanner.cpp
    src/nodbc_extension.cpp
    src/odbc_exports.cpp)

# Build the extension
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link against ODBC libraries and nanodbc for all targets
target_link_libraries(${EXTENSION_NAME} ${ODBC_LIBRARIES} nanodbc)
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${ODBC_LIBRARIES} nanodbc)

# Install the extension
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")