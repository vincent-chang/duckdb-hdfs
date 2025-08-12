#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "hadoopfs_extension.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb
{
    static void LoadInternal(DatabaseInstance &instance)
    {
        auto &fs = instance.GetFileSystem();
        fs.RegisterSubSystem(duckdb::make_uniq<HadoopFileSystem>(instance));
    }

    void HadoopfsExtension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }

    std::string HadoopfsExtension::Name()
    {
        return "hadoopfs";
    }

} // namespace duckdb

extern "C"
{

    DUCKDB_EXTENSION_API void hadoopfs_init(duckdb::DatabaseInstance &db)
    {
        LoadInternal(db);
    }

    DUCKDB_EXTENSION_API const char *hadoopfs_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
