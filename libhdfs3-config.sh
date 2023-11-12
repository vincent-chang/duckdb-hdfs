#!/bin/bash
if [ ! -d "third_party/libhdfs3" ]; then
  url="https://github.com/vincent-chang/libhdfs3/releases/download/v2.2.30/libhdfs3.$(uname)_$(arch).tgz"
  if [ -n "$url" ]; then
    mkdir "third_party/libhdfs3"
    curl -s -v --retry 5 -L $url | tar xz --strip-components 1 -C third_party/libhdfs3
  fi
fi
    