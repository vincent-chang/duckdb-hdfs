#pragma once

#include "duckdb.hpp"
#include "hadoopfs.hpp"

namespace duckdb {

class HadoopfsExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
};

} // namespace duckdb