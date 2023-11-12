import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/'))
    for x in ['extension/hadoopfs/include', 'third_party/libhdfs3/include/hdfs']
]
# source files
source_files = [
    os.path.sep.join(x.split('/'))
    for x in ['extension/hadoopfs/' + s for s in ['hadoopfs_extension.cpp', 'hadoopfs.cpp']]
]
