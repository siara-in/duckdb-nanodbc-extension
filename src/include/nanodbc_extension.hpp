#pragma once

#include "duckdb.hpp"

namespace duckdb {

class NanodbcExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override {
        return "nanodbc";
    }
    std::string Version() const override {
#ifdef EXT_VERSION_NANODBC
        return EXT_VERSION_NANODBC;
#else
        return "";
#endif
    }
};

} // namespace duckdb