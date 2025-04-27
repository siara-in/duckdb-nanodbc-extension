#include "odbc_encoding.hpp"
#include "duckdb/common/string_util.hpp"
#include <algorithm>

#ifdef _WIN32
#include <windows.h>
#else
#include <iconv.h>
#include <errno.h>
#endif

namespace duckdb {

// Initialize encoding map
const std::unordered_map<std::string, int> OdbcEncoding::encoding_to_codepage = 
    OdbcEncoding::InitializeEncodingMap();

const std::unordered_map<std::string, int> OdbcEncoding::InitializeEncodingMap() {
    std::unordered_map<std::string, int> map;
    
    // UTF-8 variants
    map["UTF-8"] = 65001;
    map["UTF8"] = 65001;
    map["CP65001"] = 65001;
    
    // Windows codepages
    map["CP1252"] = 1252;
    map["WINDOWS-1252"] = 1252;
    map["CP1250"] = 1250;
    map["WINDOWS-1250"] = 1250;
    map["CP1251"] = 1251;
    map["WINDOWS-1251"] = 1251;
    map["CP1253"] = 1253;
    map["WINDOWS-1253"] = 1253;
    map["CP1254"] = 1254;
    map["WINDOWS-1254"] = 1254;
    map["CP1255"] = 1255;
    map["WINDOWS-1255"] = 1255;
    map["CP1256"] = 1256;
    map["WINDOWS-1256"] = 1256;
    map["CP1257"] = 1257;
    map["WINDOWS-1257"] = 1257;
    map["CP1258"] = 1258;
    map["WINDOWS-1258"] = 1258;
    
    // ISO encodings
    map["ISO-8859-1"] = 28591;
    map["ISO-8859-2"] = 28592;
    map["ISO-8859-3"] = 28593;
    map["ISO-8859-4"] = 28594;
    map["ISO-8859-5"] = 28595;
    map["ISO-8859-6"] = 28596;
    map["ISO-8859-7"] = 28597;
    map["ISO-8859-8"] = 28598;
    map["ISO-8859-9"] = 28599;
    map["ISO-8859-15"] = 28605;
    
    // Asian encodings
    map["SHIFT_JIS"] = 932;
    map["SHIFT-JIS"] = 932;
    map["CP932"] = 932;
    map["GB2312"] = 936;
    map["CP936"] = 936;
    map["GBK"] = 936;
    map["BIG5"] = 950;
    map["CP950"] = 950;
    map["EUC-KR"] = 949;
    map["CP949"] = 949;
    
    return map;
}

std::string OdbcEncoding::NormalizeEncodingName(const std::string& encoding) {
    std::string normalized = StringUtil::Upper(encoding);
    
    // Remove common prefixes
    if (normalized.substr(0, 3) == "CP_") {
        normalized = "CP" + normalized.substr(3);
    }
    
    // Replace underscores with hyphens
    std::replace(normalized.begin(), normalized.end(), '_', '-');
    
    return normalized;
}

bool OdbcEncoding::NeedsConversion(const std::string& encoding) {
    std::string normalized = NormalizeEncodingName(encoding);
    return normalized != "UTF-8" && normalized != "UTF8";
}

int OdbcEncoding::GetWindowsCodepage(const std::string& encoding) {
    std::string normalized = NormalizeEncodingName(encoding);
    
    auto it = encoding_to_codepage.find(normalized);
    if (it != encoding_to_codepage.end()) {
        return it->second;
    }
    
    // Try to parse as a number
    try {
        return std::stoi(encoding);
    } catch (...) {
        // Default to system ANSI codepage
        return 0;  // CP_ACP
    }
}

std::string OdbcEncoding::ConvertToUTF8(const std::string& input, const std::string& from_encoding) {
    if (input.empty() || !NeedsConversion(from_encoding)) {
        return input;
    }
    
#ifdef _WIN32
    int codepage = GetWindowsCodepage(from_encoding);
    return ConvertToUTF8_Windows(input, codepage);
#else
    return ConvertToUTF8_Unix(input, from_encoding);
#endif
}

#ifdef _WIN32
std::string OdbcEncoding::ConvertToUTF8_Windows(const std::string& input, int codepage) {
    if (input.empty()) {
        return input;
    }
    
    // First, convert from specified codepage to UTF-16
    int wide_size = MultiByteToWideChar(codepage, MB_ERR_INVALID_CHARS,
                                      input.c_str(), -1, nullptr, 0);
    
    if (wide_size == 0) {
        // If conversion fails, return original string
        return input;
    }
    
    std::vector<wchar_t> wide_str(wide_size);
    if (MultiByteToWideChar(codepage, MB_ERR_INVALID_CHARS, 
                           input.c_str(), -1, 
                           wide_str.data(), wide_size) == 0) {
        // If conversion fails, return original string
        return input;
    }
    
    // Then convert from UTF-16 to UTF-8
    int utf8_size = WideCharToMultiByte(CP_UTF8, 0,
                                       wide_str.data(), -1,
                                       nullptr, 0, nullptr, nullptr);
    
    if (utf8_size == 0) {
        // If conversion fails, return original string
        return input;
    }
    
    std::vector<char> utf8_str(utf8_size);
    if (WideCharToMultiByte(CP_UTF8, 0,
                           wide_str.data(), -1,
                           utf8_str.data(), utf8_size,
                           nullptr, nullptr) == 0) {
        // If conversion fails, return original string
        return input;
    }
    
    // Remove null terminator if present
    if (utf8_size > 0 && utf8_str[utf8_size - 1] == '\0') {
        return std::string(utf8_str.data(), utf8_size - 1);
    }
    
    return std::string(utf8_str.data(), utf8_size);
}
#else
std::string OdbcEncoding::ConvertToUTF8_Unix(const std::string& input, const std::string& encoding) {
    if (input.empty()) {
        return input;
    }
    
    // On Unix systems, use iconv for conversion
    iconv_t cd = iconv_open("UTF-8", encoding.c_str());
    if (cd == (iconv_t)-1) {
        // If we can't open the conversion, return original string
        return input;
    }
    
    // Prepare input and output buffers
    size_t in_bytes_left = input.length();
    size_t out_bytes_left = in_bytes_left * 4; // UTF-8 can take up to 4 bytes per character
    
    std::vector<char> out_buffer(out_bytes_left);
    
    char* in_ptr = const_cast<char*>(input.c_str());
    char* out_ptr = out_buffer.data();
    
    // Perform conversion
    size_t result = iconv(cd, &in_ptr, &in_bytes_left, &out_ptr, &out_bytes_left);
    
    iconv_close(cd);
    
    if (result == (size_t)-1) {
        // Conversion failed, return original string
        return input;
    }
    
    // Calculate actual output size
    size_t output_size = out_buffer.size() - out_bytes_left;
    
    return std::string(out_buffer.data(), output_size);
}
#endif

} // namespace duckdb