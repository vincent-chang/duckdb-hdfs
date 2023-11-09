#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "hdfs.h"

namespace duckdb {

    using HeaderMap = case_insensitive_map_t<string>;

    struct HDFSEnvironmentSettingsProvider {
        static constexpr const char *HDFS_DEFAULT_NAMENODE = "HDFS_DEFAULT_NAMENODE";
        static constexpr const char *HDFS_PRINCIPAL = "HDFS_PRINCIPAL";
        static constexpr const char *HDFS_KEYTAB_FILE = "HDFS_KEYTAB_FILE";

        explicit HDFSEnvironmentSettingsProvider(DBConfig &config) : config(config) {};

        DBConfig &config;

        void SetExtensionOptionValue(string key, const char *env_var);

        void SetAll();
    };

    struct HDFSParams {
        static constexpr const char  *HDFS_DEFAULT_NAMENODE = "hdfs_default_namenode";

        string default_namenode;

        static HDFSParams ReadFrom(DatabaseInstance &instance);
        static HDFSParams ReadFrom(FileOpener *opener, FileOpenerInfo &info);
    };

    struct HDFSKerberosParams {
        static constexpr const char *HDFS_PRINCIPAL = "hdfs_principal";
        static constexpr const char *HDFS_KEYTAB_FILE = "hdfs_keytab_file";

        string principal;
        string keytab_file;

        static HDFSKerberosParams ReadFrom(DatabaseInstance &instance);
        static HDFSKerberosParams ReadFrom(FileOpener *opener, FileOpenerInfo &info);
    };

    class HadoopFileSystem;

    class HadoopFileHandle : public FileHandle {
        friend class HadoopFileSystem;

    public:
        HadoopFileHandle(FileSystem &fs, string path, uint8_t flags, hdfsFS hdfs);

        // This two-phase construction allows subclasses more flexible setup.
        virtual void Initialize(FileOpener *opener);

        hdfsFS  hdfs = nullptr;
        hdfsFile hdfs_file = nullptr;
        FileType file_type = FileType::FILE_TYPE_INVALID;

        // File handle info
        uint8_t flags;
        idx_t length;
        time_t last_modified;

    public:
        void Close() override;

    };

    class HadoopFileSystem : public FileSystem {
    public:
        static void ParseUrl(const string &url, string &path_out, string &proto_host_port_out);

        explicit HadoopFileSystem(DatabaseInstance &db);

        duckdb::unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = DEFAULT_LOCK,
                                                FileCompressionType compression = DEFAULT_COMPRESSION,
                                                FileOpener *opener = nullptr) final;

        bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                       FileOpener *opener) override;

        vector<string> Glob(const string &path, FileOpener *opener = nullptr) override;

        FileType GetFileType(FileHandle &handle) override;

        void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

        int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

        void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

        int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

        void FileSync(FileHandle &handle) override;

        void Truncate(FileHandle &handle, int64_t new_size) override;

        bool DirectoryExists(const string &directory) override;

        void CreateDirectory(const string &directory) override;

        void RemoveDirectory(const string &directory) override;

        void MoveFile(const string &source, const string &target) override;

        void RemoveFile(const string &filename) override;

        void Reset(FileHandle &handle) override;

        int64_t GetFileSize(FileHandle &handle) override;

        time_t GetLastModifiedTime(FileHandle &handle) override;

        bool FileExists(const string &filename) override;

        void Seek(FileHandle &handle, idx_t location) override;

        idx_t SeekPosition(FileHandle &handle) override;

        bool CanHandleFile(const string &fpath) override;

        bool CanSeek() override {
            return true;
        }

        bool OnDiskFile(FileHandle &handle) override {
            return false;
        }

        bool IsPipe(const string &filename) override {
            return false;
        }

        string GetName() const override {
            return "HadoopFileSystem";
        }

        string PathSeparator(const string &path) override {
            return "/";
        }

        ~HadoopFileSystem() override;


    protected:
        virtual duckdb::unique_ptr<HadoopFileHandle> CreateHandle(const string &path, uint8_t flags, FileLockType lock,
                                                                  FileCompressionType compression, FileOpener *opener);
    private:
        DatabaseInstance &instance;
        hdfsFS hdfs;
    };

} // namespace duckdb
