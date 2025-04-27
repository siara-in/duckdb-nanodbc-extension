#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>
#include <unordered_map>

namespace duckdb {

class OdbcEncoding {
public:
    // Convert from specified encoding to UTF-8
    static std::string ConvertToUTF8(const std::string& input, const std::string& from_encoding);
    
    // Check if conversion is needed
    static bool NeedsConversion(const std::string& encoding);
    
    // Normalize encoding name (e.g., "utf8" -> "UTF-8")
    static std::string NormalizeEncodingName(const std::string& encoding);
    
    // Get codepage for Windows encoding name
    static int GetWindowsCodepage(const std::string& encoding);

private:
    // Initialize the encoding map
    static const std::unordered_map<std::string, int> InitializeEncodingMap();
    
    // Platform-specific conversion functions
#ifdef _WIN32
    static std::string ConvertToUTF8_Windows(const std::string& input, int codepage);
#else
    static std::string ConvertToUTF8_Unix(const std::string& input, const std::string& encoding);
#endif
    
    // Encoding name to Windows codepage map
    static const std::unordered_map<std::string, int> encoding_to_codepage;
};

} // namespace duckdb