# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

function(crowdb_rpc_instrument_folly)
  include(FetchContent)
  if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
  endif()
  list(PREPEND CMAKE_PREFIX_PATH "$ENV{CONDA_PREFIX}")
  # Match the pinned dependency. Its singleton publication and hazard-pointer
  # synchronization must be visible to the same sanitizer as the consumer.
  FetchContent_Declare(crowdb_folly
    URL https://github.com/facebook/folly/archive/v2026.08.10.00.tar.gz
    URL_HASH SHA256=add1cde8feb39f2f3777bf222f64e6ebf6d8107806e6b665afa3772a7acf8b74
    SOURCE_SUBDIR crowdb-download-only)
  FetchContent_Declare(crowdb_fast_float
    URL https://github.com/fastfloat/fast_float/archive/refs/tags/v8.0.0.tar.gz
    URL_HASH SHA256=f312f2dc34c61e665f4b132c0307d6f70ad9420185fa831911bc24408acf625d
    SOURCE_SUBDIR crowdb-download-only)
  FetchContent_MakeAvailable(crowdb_folly crowdb_fast_float)
  set(FASTFLOAT_INCLUDE_DIR "${crowdb_fast_float_SOURCE_DIR}/include")
  set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
  set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
  set(CMAKE_PROJECT_folly_INCLUDE "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/folly-project.cmake")
  set(BUILD_TESTS OFF)
  set(BUILD_BENCHMARKS OFF)
  set(BUILD_SHARED_LIBS OFF)
  set(BOOST_LINK_STATIC OFF)
  set(ENABLE_JEMALLOC OFF)
  add_subdirectory("${crowdb_folly_SOURCE_DIR}" "${crowdb_folly_BINARY_DIR}" EXCLUDE_FROM_ALL)
  # Link only the map and its real dependencies; all are compiled with TSan.
  target_link_libraries(crowdb-rpc PUBLIC folly_concurrency_concurrent_hash_map)
  target_include_directories(crowdb-rpc SYSTEM PUBLIC
    "${crowdb_folly_SOURCE_DIR}" "${crowdb_folly_BINARY_DIR}")
endfunction()

crowdb_rpc_instrument_folly()
