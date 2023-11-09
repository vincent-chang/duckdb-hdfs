#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "hadoopfs_extension.hpp"

namespace duckdb {

    static void LoadInternal(DatabaseInstance &instance) {

        auto &config = DBConfig::GetConfig(instance);

        // Global HDFS config
        config.AddExtensionOption("hdfs_default_namenode", "HDFS default namenode", LogicalType::VARCHAR);
        config.AddExtensionOption("hdfs_principal", "HDFS kerberos principal", LogicalType::VARCHAR);
        config.AddExtensionOption("hdfs_keytab_file", "HDFS kerberos keytab file", LogicalType::VARCHAR);

        auto provider = make_uniq<HDFSEnvironmentSettingsProvider>(config);
        provider->SetAll();

        auto &fs = instance.GetFileSystem();
        fs.RegisterSubSystem(make_uniq<HadoopFileSystem>(instance));
    }

    void HadoopfsExtension::Load(DuckDB &db) {
        LoadInternal(*db.instance);
    }

    std::string HadoopfsExtension::Name() {
        return "hadoopfs";
    }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void hadoopfs_init(duckdb::DatabaseInstance &db) {
    LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *hadoopfs_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
