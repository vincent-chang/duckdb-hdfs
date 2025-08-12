#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_data.hpp"

#include <hdfs/hdfs.h>

namespace duckdb
{

    struct HDFSParams
    {
        static constexpr const char *HDFS_DEFAULT_NAMENODE = "hdfs_default_namenode";
        static constexpr const char *HDFS_HA_NAMENODES = "hdfs_ha_namenodes";
        static constexpr const char *HDFS_SHORTCIRCUIT = "hdfs_shortcircuit";
        static constexpr const char *HDFS_DOMAIN_SOCKET_PATH = "hdfs_domain_socket_path";

        string namenode = "default";
        string url = "";
        vector<string> ha_namenodes;
        bool shortcircuit;
        string domain_socket_path;

        template <typename T>
        static HDFSParams ReadFrom(T &config)
        {
            string namenode = "";
            vector<string> ha_namenodes;
            bool shortcircuit = false;
            string domain_socket_path = "";
            Value value;

            if (config.TryGetCurrentSetting(HDFSParams::HDFS_DEFAULT_NAMENODE, value))
            {
                namenode = StringUtil::Lower(value.ToString());
                if (StringUtil::StartsWith(namenode, "hdfs://"))
                {
                    auto slash_pos = namenode.find('/', 8);
                    if (slash_pos == string::npos)
                    {
                        namenode = namenode.substr(7);
                    }
                    else
                    {
                        namenode = namenode.substr(7, slash_pos - 7);
                    }
                }
            }
            if (config.TryGetCurrentSetting(HDFSParams::HDFS_HA_NAMENODES, value))
            {
                string tmp_namenodes = value.ToString();
                StringUtil::Trim(tmp_namenodes);
                if (!tmp_namenodes.empty()) 
                {
                    ha_namenodes = StringUtil::Split(StringUtil::Lower(tmp_namenodes), ",");
                }
            }
            if (config.TryGetCurrentSetting(HDFSParams::HDFS_SHORTCIRCUIT, value))
            {
                shortcircuit = value.GetValue<bool>();
            }
            if (config.TryGetCurrentSetting(HDFSParams::HDFS_DOMAIN_SOCKET_PATH, value))
            {
                domain_socket_path = value.ToString();
            }
            HDFSParams result;
            result.namenode = namenode;
            result.url = "";
            result.ha_namenodes = ha_namenodes;
            result.shortcircuit = shortcircuit;
            result.domain_socket_path = domain_socket_path;
            return result;
        }
        // static HDFSParams ReadFrom(DatabaseInstance &instance);
        // static HDFSParams ReadFrom(FileOpener *opener, FileOpenerInfo &info);
    };

    struct HDFSEnvironmentSettingsProvider
    {
        static constexpr const char *HDFS_DEFAULT_NAMENODE = "HDFS_DEFAULT_NAMENODE";
        static constexpr const char *HDFS_HA_NAMENODES = "HDFS_HA_NAMENODES";
        static constexpr const char *HDFS_SHORTCIRCUIT = "HDFS_SHORTCIRCUIT";
        static constexpr const char *HDFS_DOMAIN_SOCKET_PATH = "HDFS_DOMAIN_SOCKET_PATH";

        explicit HDFSEnvironmentSettingsProvider(DBConfig &config) : config(config){};

        DBConfig &config;

        void SetExtensionOptionValue(string key, const char *env_var_name)
        {
            static char *evar;
            if ((evar = std::getenv(env_var_name)) != NULL)
            {
                if (StringUtil::Lower(evar) == "false")
                {
                    this->config.SetOption(key, Value(false));
                }
                else if (StringUtil::Lower(evar) == "true")
                {
                    this->config.SetOption(key, Value(true));
                }
                else
                {
                    this->config.SetOption(key, Value(evar));
                }
            }
        }

        void SetAll()
        {
            this->SetExtensionOptionValue(HDFSParams::HDFS_DEFAULT_NAMENODE, this->HDFS_DEFAULT_NAMENODE);
            this->SetExtensionOptionValue(HDFSParams::HDFS_HA_NAMENODES, this->HDFS_HA_NAMENODES);
            this->SetExtensionOptionValue(HDFSParams::HDFS_SHORTCIRCUIT, this->HDFS_SHORTCIRCUIT);
            this->SetExtensionOptionValue(HDFSParams::HDFS_DOMAIN_SOCKET_PATH, this->HDFS_DOMAIN_SOCKET_PATH);
        }
    };

    class HadoopFileSystem;

    class HadoopFileHandle : public FileHandle
    {
        friend class HadoopFileSystem;

    public:
        HadoopFileHandle(FileSystem &fs, string path, FileOpenFlags flags, hdfsFS hdfs);
        virtual ~HadoopFileHandle() override;
        // This two-phase construction allows subclasses more flexible setup.
        virtual void Initialize(FileOpener *opener);

        hdfsFS hdfs = nullptr;
        hdfsFile hdfs_file = nullptr;
        FileType file_type = FileType::FILE_TYPE_INVALID;

        // File handle info
        FileOpenFlags flags;
        idx_t length;
        time_t last_modified{};

    public:
        void Close() override;
    };

    class HadoopFileSystem : public FileSystem
    {
    public:
        static void ParseUrl(const string &url, string &path_out, string &proto_host_port_out);

        static void ParseUrl(const string &url, string &path_out, HDFSParams &hdfs_params_out);

        static bool Match(FileType file_type,
                          vector<string>::const_iterator key, vector<string>::const_iterator key_end,
                          vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end);

        explicit HadoopFileSystem(DatabaseInstance &db);

        hdfsFS GetHadoopFileSystem();

        hdfsFS GetHadoopFileSystemWithException();

        hdfsFS GetHadoopFileSystem(const string &url, optional_ptr<FileOpener> opener);

        hdfsFS GetHadoopFileSystem(const HDFSParams &hdfs_params);

        unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override;

        bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                       FileOpener *opener) override;

        //vector<string> Glob(const string &path, FileOpener *opener = nullptr) override;
        vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

        FileType GetFileType(FileHandle &handle) override;

        void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

        int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

        int64_t ReadFromHDFS(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);

        int64_t ReadFromHDFS(FileHandle &handle, void *buffer, int64_t nr_bytes);

        void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

        int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

        void FileSync(FileHandle &handle) override;

        void Truncate(FileHandle &handle, int64_t new_size) override;

        bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;

        void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;

        void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;

        void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;

        void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

        void Reset(FileHandle &handle) override;

        int64_t GetFileSize(FileHandle &handle) override;

        time_t GetLastModifiedTime(FileHandle &handle) override;

        bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

        void Seek(FileHandle &handle, idx_t location) override;

        idx_t SeekPosition(FileHandle &handle) override;

        bool CanHandleFile(const string &fpath) override;

        bool CanSeek() override
        {
            return true;
        }

        bool OnDiskFile(FileHandle &handle) override
        {
            return false;
        }

        bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override
        {
            return false;
        }

        string GetName() const override
        {
            return "HadoopFileSystem";
        }

        string PathSeparator(const string &path) override
        {
            return "/";
        }

        ~HadoopFileSystem() override;

    protected:
        virtual unique_ptr<HadoopFileHandle> CreateHandle(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr);

    private:
        DatabaseInstance &instance;
        std::map<std::string, hdfsFS> hdfs_map;

        hdfsFS GetHadoopFileSystemFromMap(const string &key);
        hdfsFS CreateAndPutHadoopFileSystemToMap(const string &key, const std::function<hdfsFS()> & get_hdfs_fs);
        hdfsFS GetHadoopFileSystem(const string &key, const std::function<hdfsFS()> & get_hdfs_fs);

    };

} // namespace duckdb