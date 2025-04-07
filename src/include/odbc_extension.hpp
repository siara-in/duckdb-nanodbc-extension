#pragma once

#include "duckdb.hpp"

namespace duckdb {

class OdbcExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override {
        return "odbc";
    }
    std::string Version() const override {
#ifdef EXT_VERSION_ODBC
        return EXT_VERSION_ODBC;
#else
        return "";
#endif
    }
};

} // namespace duckdb