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
  # Find specific ODBC libraries that we know exist on the system
  find_library(ODBC_LIBRARY NAMES odbc PATHS /usr/lib/x86_64-linux-gnu)
  find_library(ODBCINST_LIBRARY NAMES odbcinst PATHS /usr/lib/x86_64-linux-gnu)
  find_library(ODBCCR_LIBRARY NAMES odbccr PATHS /usr/lib/x86_64-linux-gnu)
  
  set(ODBC_LIBRARIES "")
  
  if(ODBC_LIBRARY)
    list(APPEND ODBC_LIBRARIES ${ODBC_LIBRARY})
    message(STATUS "Found ODBC library: ${ODBC_LIBRARY}")
  else()
    message(FATAL_ERROR "Main ODBC library not found")
  endif()
  
  if(ODBCINST_LIBRARY)
    list(APPEND ODBC_LIBRARIES ${ODBCINST_LIBRARY})
    message(STATUS "Found ODBC installer library: ${ODBCINST_LIBRARY}")
  endif()
  
  if(ODBCCR_LIBRARY)
    list(APPEND ODBC_LIBRARIES ${ODBCCR_LIBRARY})
    message(STATUS "Found ODBC cursor library: ${ODBCCR_LIBRARY}")
  endif()
  
  # Also try finding via the FindODBC module (as fallback)
  find_package(ODBC)
  if(ODBC_FOUND)
    include_directories(${ODBC_INCLUDE_DIRS})
    list(APPEND ODBC_LIBRARIES ${ODBC_LIBRARIES})
  endif()
  
  message(STATUS "Final ODBC libraries for linking: ${ODBC_LIBRARIES}")
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

# Link against ODBC libraries
target_link_libraries(${EXTENSION_NAME} ${ODBC_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${ODBC_LIBRARIES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")