#pragma once

#include "duckdb.hpp"

const char *nanodbc_ver = "1.0.1";

namespace duckdb {

class NanodbcExtension : public Extension {
public:
	void Load(duckdb::ExtensionLoader &loader) override;
    std::string Name() override { return "nanodbc"; }
    std::string Version() const override { return nanodbc_ver; }
};

} // namespace duckdb