# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(hadoopfs
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/include
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    # LOAD_TESTS
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)

duckdb_extension_load(autocomplete)
duckdb_extension_load(core_functions)
duckdb_extension_load(icu)
duckdb_extension_load(json)
duckdb_extension_load(parquet)