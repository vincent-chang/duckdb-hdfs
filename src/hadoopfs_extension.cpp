#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "hadoopfs_extension.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb
{
    static void LoadInternal(ExtensionLoader &loader)
    {
        DatabaseInstance &instance = loader.GetDatabaseInstance();
        auto &fs = instance.GetFileSystem();
        fs.RegisterSubSystem(duckdb::make_uniq<HadoopFileSystem>(instance));
    }

    void HadoopfsExtension::Load(ExtensionLoader &loader)
    {
        LoadInternal(loader);
    }

    std::string HadoopfsExtension::Name()
    {
        return "hadoopfs";
    }

} // namespace duckdb

DUCKDB_CPP_EXTENSION_ENTRY(hadoopfs, loader) {
    duckdb::LoadInternal(loader);
}
