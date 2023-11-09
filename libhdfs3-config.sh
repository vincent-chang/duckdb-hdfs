#!/bin/bash
if [ ! -d "third_party/libhdfs3" ]; then
  architecture=$(arch)
  echo $architecture
  if [ "${architecture}" = "x86_64" ]; then
    url="https://github.com/vincent-chang/libhdfs3/releases/download/v2.2.30/libhdfs3.linux_amd64.tgz"
  elif [[ "${architecture}" = "aarch64" || "${architecture}" = "arm64" ]]; then
    url="https://github.com/vincent-chang/libhdfs3/releases/download/v2.2.30/libhdfs3.linux_aarch64.tgz"
  else
    url=""
  fi
  if [ -n "$url" ]; then
    mkdir "third_party/libhdfs3"
    curl -s -v --retry 5 -L $url | tar xz --strip-components 1 -C third_party/libhdfs3
  fi
fi
    