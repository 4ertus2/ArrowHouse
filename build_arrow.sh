#!/bin/bash

SRC_ROOT=$HOME/src
TPARTY=$SRC_ROOT/arrow-thirdparty
INSTALL_DIR=$HOME/local

# Environment variables for offline Arrow build
export ARROW_ABSL_URL=$TPARTY/absl-20211102.0.tar.gz
export ARROW_AWS_C_AUTH_URL=$TPARTY/aws-c-auth-v0.6.22.tar.gz
export ARROW_AWS_C_CAL_URL=$TPARTY/aws-c-cal-v0.5.20.tar.gz
export ARROW_AWS_C_COMMON_URL=$TPARTY/aws-c-common-v0.8.9.tar.gz
export ARROW_AWS_C_COMPRESSION_URL=$TPARTY/aws-c-compression-v0.2.16.tar.gz
export ARROW_AWS_C_EVENT_STREAM_URL=$TPARTY/aws-c-event-stream-v0.2.18.tar.gz
export ARROW_AWS_C_HTTP_URL=$TPARTY/aws-c-http-v0.7.3.tar.gz
export ARROW_AWS_C_IO_URL=$TPARTY/aws-c-io-v0.13.14.tar.gz
export ARROW_AWS_C_MQTT_URL=$TPARTY/aws-c-mqtt-v0.8.4.tar.gz
export ARROW_AWS_C_S3_URL=$TPARTY/aws-c-s3-v0.2.3.tar.gz
export ARROW_AWS_C_SDKUTILS_URL=$TPARTY/aws-c-sdkutils-v0.1.6.tar.gz
export ARROW_AWS_CHECKSUMS_URL=$TPARTY/aws-checksums-v0.1.13.tar.gz
export ARROW_AWS_CRT_CPP_URL=$TPARTY/aws-crt-cpp-v0.18.16.tar.gz
export ARROW_AWS_LC_URL=$TPARTY/aws-lc-v1.3.0.tar.gz
export ARROW_AWSSDK_URL=$TPARTY/aws-sdk-cpp-1.10.55.tar.gz
export ARROW_BOOST_URL=$TPARTY/boost-1.81.0.tar.gz
export ARROW_BROTLI_URL=$TPARTY/brotli-v1.0.9.tar.gz
export ARROW_BZIP2_URL=$TPARTY/bzip2-1.0.8.tar.gz
export ARROW_CARES_URL=$TPARTY/cares-1.17.2.tar.gz
export ARROW_CRC32C_URL=$TPARTY/crc32c-1.1.2.tar.gz
export ARROW_GBENCHMARK_URL=$TPARTY/gbenchmark-v1.7.1.tar.gz
export ARROW_GFLAGS_URL=$TPARTY/gflags-v2.2.2.tar.gz
export ARROW_GLOG_URL=$TPARTY/glog-v0.5.0.tar.gz
export ARROW_GOOGLE_CLOUD_CPP_URL=$TPARTY/google-cloud-cpp-v2.12.0.tar.gz
export ARROW_GRPC_URL=$TPARTY/grpc-v1.46.3.tar.gz
export ARROW_GTEST_URL=$TPARTY/gtest-1.11.0.tar.gz
export ARROW_JEMALLOC_URL=$TPARTY/jemalloc-5.3.0.tar.bz2
export ARROW_LZ4_URL=$TPARTY/lz4-v1.9.4.tar.gz
export ARROW_MIMALLOC_URL=$TPARTY/mimalloc-v2.0.6.tar.gz
export ARROW_NLOHMANN_JSON_URL=$TPARTY/nlohmann-json-v3.10.5.tar.gz
export ARROW_OPENTELEMETRY_URL=$TPARTY/opentelemetry-cpp-v1.8.1.tar.gz
export ARROW_OPENTELEMETRY_PROTO_URL=$TPARTY/opentelemetry-proto-v0.17.0.tar.gz
export ARROW_ORC_URL=$TPARTY/orc-1.9.2.tar.gz
export ARROW_PROTOBUF_URL=$TPARTY/protobuf-v21.3.tar.gz
export ARROW_RAPIDJSON_URL=$TPARTY/rapidjson-232389d4f1012dddec4ef84861face2d2ba85709.tar.gz
export ARROW_RE2_URL=$TPARTY/re2-2022-06-01.tar.gz
export ARROW_S2N_TLS_URL=$TPARTY/s2n-v1.3.35.tar.gz
export ARROW_SNAPPY_URL=$TPARTY/snappy-1.1.10.tar.gz
export ARROW_THRIFT_URL=$TPARTY/thrift-0.16.0.tar.gz
export ARROW_UCX_URL=$TPARTY/ucx-1.12.1.tar.gz
export ARROW_UTF8PROC_URL=$TPARTY/utf8proc-v2.7.0.tar.gz
export ARROW_XSIMD_URL=$TPARTY/xsimd-9.0.1.tar.gz
export ARROW_ZLIB_URL=$TPARTY/zlib-1.3.1.tar.gz
export ARROW_ZSTD_URL=$TPARTY/zstd-1.5.5.tar.gz

BUILD_TYPE="-DCMAKE_BUILD_TYPE=RelWithDebInfo -DARROW_SIMD_LEVEL=AVX2 -DARROW_RUNTIME_SIMD_LEVEL=AVX2 -DARROW_CXXFLAGS='-fno-omit-frame-pointer'"
BUILD_OPTS="-DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF -DARROW_ENABLE_THREADING=OFF -DARROW_NO_DEPRECATED_API=ON"
#COMPUTE="-DARROW_COMPUTE=ON -DARROW_GANDIVA=ON -DARROW_ACERO=ON"
COMPUTE="-DARROW_COMPUTE=ON"
#FORMATS="-DARROW_CSV=ON -DARROW_JSON=ON -DARROW_PARQUET=ON"
FORMATS="-DARROW_PARQUET=ON"
COMPRESSION="-DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZSTD=ON"
#LIBS="-DARROW_FILESYSTEM=ON -DARROW_S3=ON -DARROW_SUBSTRAIT=ON -DARROW_IPC=ON"

cd $SRC_ROOT/arrow/cpp/ && rm -rf _build
mkdir _build && cd _build
cmake -GNinja $BUILD_TYPE $BUILD_OPTS $COMPUTE $FORMATS $COMPRESSION $LIBS ../
ninja
cmake --install . --prefix $INSTALL_DIR
