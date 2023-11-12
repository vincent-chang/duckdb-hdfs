# DuckDB hdfs extension
Grant DuckDB the capability to access HDFS files.

## Usage
```SQL
select * from 'hdfs://localhost:9000/path/to/file';
```

## Building & Loading the Extension

To build, type
```
make
```

To run, run the bundled `duckdb` shell:
```
 ./build/release/duckdb -unsigned  # allow unsigned extensions
```

Then, load the Postgres extension like so:
```SQL
LOAD 'build/release/extension/hadoopfs/hadoopfs.duckdb_extension';
```

## License

This project is licensed under the MIT License. For more details, see the [LICENSE](https://github.com/vincent-chang/duckdb-hdfs/blob/main/LICENSE) file.
