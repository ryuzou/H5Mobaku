cmake_minimum_required(VERSION 3.26)
project(H5MobakuReader C CXX)

project(H5mobakuReader VERSION 0.1.0 LANGUAGES C CXX)
set(H5MR_VERSION      ${PROJECT_VERSION})
set(H5MR_SOVERSION    0)
set(H5MR_EXPORT       H5MRTargets)

option(H5MR_BUILD_SHARED   "Build shared library"      ON)
option(H5MR_BUILD_STATIC   "Build static library"      ON)
option(H5MR_BUILD_TESTS    "Build tests"               ON)
option(H5MR_ENABLE_PYBIND  "Build pybind wrapper"      OFF)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_C_FLAGS_DEBUG "-g -O0 -DDEBUG")
set(CMAKE_C_FLAGS_RELEASE "-O3")

find_package(HDF5 REQUIRED COMPONENTS C HL)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)

#cmph
set(LOCAL_INCLUDE /usr/local)

if(NOT EXISTS ${LOCAL_INCLUDE}/include/cmph.h)
    message(FATAL_ERROR "CMPH header files not found at ${LOCAL_INCLUDE}/include")
endif()

if(NOT EXISTS ${LOCAL_INCLUDE}/lib/libcmph.so)
    message(FATAL_ERROR "CMPH library not found at ${LOCAL_INCLUDE}/lib")
endif()

set(CMPH_INCLUDE_DIRS ${LOCAL_INCLUDE}/include)
set(CMPH_LIBRARIES cmph)

SET(OBJS
        ${CMAKE_CURRENT_SOURCE_DIR}/external/meshids/meshid_mobaku.o
        ${CMAKE_CURRENT_SOURCE_DIR}/external/meshids/meshid_mobaku_mph.o
)

# ───────────────────────────────────
#  Internal
# ───────────────────────────────────
add_library(h5mr_internal OBJECT
        src/h5mr_core.c
        src/meshid_ops.c
        src/h5mobaku_ops.c
        src/env_utils.c
)
target_include_directories(h5mr_internal
        PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE ${LIBURING_INCLUDE_DIRS}
        ${CMPH_INCLUDE_DIRS}
)
target_link_libraries(h5mr_internal
        PUBLIC ${HDF5_C_LIBRARIES}
        PRIVATE ${LIBURING_LIBRARIES} ${CMPH_LIBRARIES}
)
target_compile_features(h5mr_internal PUBLIC c_std_11)

# ───────────────────────────────────
#  External Libraries
# ───────────────────────────────────
set(H5MR_MAIN_TARGET "")

if(H5MR_BUILD_SHARED)
    add_library(h5mr SHARED $<TARGET_OBJECTS:h5mr_internal> ${OBJS})
    target_include_directories(h5mr
            PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
            PRIVATE ${LIBURING_INCLUDE_DIRS}
            ${CMPH_INCLUDE_DIRS}
    )
    target_link_libraries(h5mr
            PUBLIC ${HDF5_C_LIBRARIES}
            PRIVATE ${LIBURING_LIBRARIES} ${CMPH_LIBRARIES}
    )
    set_target_properties(h5mr PROPERTIES
            OUTPUT_NAME h5mr
            VERSION     ${H5MR_VERSION}
            SOVERSION   ${H5MR_SOVERSION}
            PUBLIC_HEADER "include/H5MR/h5mr.h"
    )
    install(TARGETS h5mr
            EXPORT   ${H5MR_EXPORT}
            LIBRARY  DESTINATION lib
            PUBLIC_HEADER DESTINATION include/H5MR
    )
    set(H5MR_MAIN_TARGET h5mr)
endif()

if(H5MR_BUILD_STATIC)
    add_library(h5mr_static STATIC $<TARGET_OBJECTS:h5mr_internal> ${OBJS})
    target_include_directories(h5mr_static
            PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
            PRIVATE ${LIBURING_INCLUDE_DIRS}
            ${CMPH_INCLUDE_DIRS}
    )
    target_link_libraries(h5mr_static
            PUBLIC ${HDF5_C_LIBRARIES}
            PRIVATE ${LIBURING_LIBRARIES} ${CMPH_LIBRARIES}
    )
    set_target_properties(h5mr_static PROPERTIES OUTPUT_NAME h5mr)
    install(TARGETS h5mr_static
            EXPORT   ${H5MR_EXPORT}
            ARCHIVE  DESTINATION lib
    )
    # 共有ライブラリがない場合は静的ライブラリをメインターゲットにする
    if(NOT H5MR_BUILD_SHARED)
        set(H5MR_MAIN_TARGET h5mr_static)
    endif()
endif()

# エイリアスを作成（共有ライブラリ優先、なければ静的ライブラリ）
if(H5MR_MAIN_TARGET)
    add_library(H5MR::h5mr ALIAS ${H5MR_MAIN_TARGET})
else()
    message(FATAL_ERROR "Neither shared nor static library is enabled")
endif()

# ───────────────────────────────────
#  For CMake Packages
# ───────────────────────────────────
include(CMakePackageConfigHelpers)
configure_package_config_file(
        cmake/H5MR-config.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/H5MR-config.cmake
        INSTALL_DESTINATION  lib/cmake/H5MR
)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/H5MR-config.cmake
        DESTINATION lib/cmake/H5MR)

install(EXPORT ${H5MR_EXPORT}
        NAMESPACE H5MR::
        DESTINATION lib/cmake/H5MR)

# ヘッダーファイルをインストール
install(DIRECTORY include/
        DESTINATION include
        FILES_MATCHING PATTERN "*.h")

# ───────────────────────────────────
#  Test
# ───────────────────────────────────
if(H5MR_BUILD_TESTS)
    enable_testing()
    add_executable(verify_layout tests/verify_layout.cpp)
    target_link_libraries(verify_layout PRIVATE H5MR::h5mr ${HDF5_C_LIBRARIES} stdc++)
    set_target_properties(verify_layout PROPERTIES
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
    )
    add_test(NAME H5MR.verify_layout COMMAND verify_layout)

    add_dependencies(verify_layout ${H5MR_MAIN_TARGET})

    # Add test_meshid_ops executable
    add_executable(test_meshid_ops tests/test_meshid_ops.c)
    target_link_libraries(test_meshid_ops PRIVATE H5MR::h5mr ${CMPH_LIBRARIES})
    target_link_directories(test_meshid_ops PRIVATE ${LOCAL_INCLUDE}/lib)
    target_include_directories(test_meshid_ops PRIVATE ${CMPH_INCLUDE_DIRS})
    set_target_properties(test_meshid_ops PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
    )
    add_test(NAME H5MR.test_meshid_ops COMMAND test_meshid_ops)
    add_dependencies(test_meshid_ops ${H5MR_MAIN_TARGET})
    
    # Add test_h5mobaku_ops executable
    add_executable(test_h5mobaku_ops tests/test_h5mobaku_ops.c)
    target_link_libraries(test_h5mobaku_ops PRIVATE H5MR::h5mr ${CMPH_LIBRARIES})
    target_link_directories(test_h5mobaku_ops PRIVATE ${LOCAL_INCLUDE}/lib)
    target_include_directories(test_h5mobaku_ops PRIVATE ${CMPH_INCLUDE_DIRS})
    set_target_properties(test_h5mobaku_ops PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
    )
    add_test(NAME H5MR.test_h5mobaku_ops COMMAND test_h5mobaku_ops)
    add_dependencies(test_h5mobaku_ops ${H5MR_MAIN_TARGET})
endif()

# ───────────────────────────────────
#  CLI Tools
# ───────────────────────────────────
option(H5MR_BUILD_CLI      "Build CLI tools"           ON)

if(H5MR_BUILD_CLI)
    add_executable(h5m-reader src/h5m-reader.c)
    target_link_libraries(h5m-reader PRIVATE H5MR::h5mr ${CMPH_LIBRARIES})
    target_link_directories(h5m-reader PRIVATE ${LOCAL_INCLUDE}/lib)
    target_include_directories(h5m-reader PRIVATE ${CMPH_INCLUDE_DIRS})
    set_target_properties(h5m-reader PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
    )
    add_dependencies(h5m-reader ${H5MR_MAIN_TARGET})
    
    # Install CLI tools
    install(TARGETS h5m-reader
            RUNTIME DESTINATION bin
    )
endif()

# ───────────────────────────────────
#  Pybind11 / torch_cpp_extension
# ───────────────────────────────────
if(H5MR_ENABLE_PYBIND)
    find_package(pybind11 CONFIG REQUIRED)
    pybind11_add_module(h5mr_py MODULE python/h5mr_pybind.cpp)
    target_link_libraries(h5mr_py PRIVATE H5MR::h5mr)
endif()