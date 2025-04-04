#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ODBCScannerExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override {
        return "odbc_scanner";
    }
};

} // namespace duckdb