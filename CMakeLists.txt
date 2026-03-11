cmake_minimum_required(VERSION 3.20)
project(LSM_KV_STORE VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(src)

add_library(lsm_kv_core
    src/dummy.cpp
)

add_executable(lsm_kv_test
    test/test_kv_store.cpp
)
target_link_libraries(lsm_kv_test PRIVATE lsm_kv_core)

enable_testing()
add_test(NAME BasicKVTest COMMAND lsm_kv_test)
