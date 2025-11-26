cmake_minimum_required(VERSION 3.26)

# Force use of clang compiler
set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)

project(H5MobakuReader C CXX)

project(H5mobakuReader VERSION 0.1.0 LANGUAGES C CXX)
set(H5MR_VERSION      ${PROJECT_VERSION})
set(H5MR_SOVERSION    0)
set(H5MR_EXPORT       H5MRTargets)

option(H5MR_BUILD_SHARED   "Build shared library"      ON)
option(H5MR_BUILD_STATIC   "Build static library"      ON)
option(H5MR_BUILD_TESTS    "Build tests"               ON)
option(H5MR_ENABLE_HEAVY_TESTS "Register resource-intensive tests (CSV/HDF5 conversion)" OFF)
option(H5MR_ENABLE_PYBIND  "Build pybind wrapper"      OFF)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)


set(CMAKE_C_FLAGS_DEBUG "-g -O0 -DDEBUG -Wall -Wextra -march=native -mavx512f")
set(CMAKE_C_FLAGS_RELEASE "-O3 -march=native -mavx512f")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG -Wall -Wextra -march=native -mavx512f")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -mavx512f")

# Copy .env file to build directory if it exists
if(EXISTS "${CMAKE_SOURCE_DIR}/.env")
    configure_file("${CMAKE_SOURCE_DIR}/.env" "${CMAKE_BINARY_DIR}/.env" COPYONLY)
endif()

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

# ───────────────────────────────────
#  Architecture-aware meshid hash generation
# ───────────────────────────────────
# Detect target architecture for objcopy
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    set(OBJCOPY_ARCH "elf64-x86-64")
    set(OBJCOPY_BINARY_ARCH "i386")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    set(OBJCOPY_ARCH "elf64-littleaarch64")
    set(OBJCOPY_BINARY_ARCH "aarch64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "armv7")
    set(OBJCOPY_ARCH "elf32-littlearm")
    set(OBJCOPY_BINARY_ARCH "arm")
else()
    message(WARNING "Unknown architecture ${CMAKE_SYSTEM_PROCESSOR}, defaulting to x86-64")
    set(OBJCOPY_ARCH "elf64-x86-64")
    set(OBJCOPY_BINARY_ARCH "i386")
endif()

# Generate meshid object files at build time
set(MESHID_GENERATED_DIR "${CMAKE_BINARY_DIR}/generated/meshids")
file(MAKE_DIRECTORY ${MESHID_GENERATED_DIR})

# Check if cmph command is available
find_program(CMPH_BINARY cmph)
if(NOT CMPH_BINARY)
    message(FATAL_ERROR "cmph command not found. Please install CMPH library tools.")
endif()

set(MESHID_MOBAKU_O "${MESHID_GENERATED_DIR}/meshid_mobaku.o")
set(MESHID_MOBAKU_MPH_O "${MESHID_GENERATED_DIR}/meshid_mobaku_mph.o")
set(MESHID_MOBAKU_MPH "${MESHID_GENERATED_DIR}/meshid_mobaku.mph")

# Generate .mph file from .txt using cmph
set(MESHID_TXT_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/external/meshids/src/meshid_1_2rd_list.txt")
add_custom_command(
    OUTPUT ${MESHID_MOBAKU_MPH}
    COMMAND ${CMPH_BINARY} -g -m ${MESHID_MOBAKU_MPH} ${MESHID_TXT_SOURCE}
    DEPENDS ${MESHID_TXT_SOURCE}
    COMMENT "Generating minimal perfect hash file meshid_mobaku.mph from meshid_1_2rd_list.txt"
    VERBATIM
)

# Compile meshid_mobaku.c -> meshid_mobaku.o
add_custom_command(
    OUTPUT ${MESHID_MOBAKU_O}
    COMMAND ${CMAKE_C_COMPILER} -c
        ${CMAKE_CURRENT_SOURCE_DIR}/external/meshids/src/meshid_mobaku.c
        -o ${MESHID_MOBAKU_O}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/external/meshids/src/meshid_mobaku.c
    COMMENT "Compiling meshid_mobaku.c for ${CMAKE_SYSTEM_PROCESSOR}"
    VERBATIM
)

# Convert meshid_mobaku.mph -> meshid_mobaku_mph.o using objcopy
# Use a wrapper script to make symbol renaming portable across different paths
add_custom_command(
    OUTPUT ${MESHID_MOBAKU_MPH_O}
    COMMAND ${CMAKE_COMMAND} -E env
        SOURCE_FILE=${MESHID_MOBAKU_MPH}
        OUTPUT_FILE=${MESHID_MOBAKU_MPH_O}
        OBJCOPY_ARCH=${OBJCOPY_ARCH}
        OBJCOPY_BINARY_ARCH=${OBJCOPY_BINARY_ARCH}
        ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/generate_mph_object.cmake
    DEPENDS ${MESHID_MOBAKU_MPH}
    COMMENT "Converting meshid_mobaku.mph to object file for ${CMAKE_SYSTEM_PROCESSOR}"
    VERBATIM
)

SET(OBJS
        ${MESHID_MOBAKU_O}
        ${MESHID_MOBAKU_MPH_O}
)

# ───────────────────────────────────
#  Internal
# ───────────────────────────────────
add_library(h5mr_internal OBJECT
        src/h5mr_core.c
        src/meshid_ops.c
        src/h5mobaku_ops.c
        src/env_utils.c
        src/csv_ops.c
        src/csv_to_h5_converter.c
        src/fifioq.c
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
    
    # Add test_csv_to_h5 executable
    add_executable(test_csv_to_h5 tests/test_csv_to_h5.c)
    target_link_libraries(test_csv_to_h5 PRIVATE H5MR::h5mr ${CMPH_LIBRARIES})
    target_link_directories(test_csv_to_h5 PRIVATE ${LOCAL_INCLUDE}/lib)
    target_include_directories(test_csv_to_h5 PRIVATE ${CMPH_INCLUDE_DIRS})
    set_target_properties(test_csv_to_h5 PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
    )
    if(H5MR_ENABLE_HEAVY_TESTS)
        add_test(NAME H5MR.test_csv_to_h5 COMMAND test_csv_to_h5)
        set_tests_properties(H5MR.test_csv_to_h5 PROPERTIES LABELS heavy)
    endif()
    add_dependencies(test_csv_to_h5 ${H5MR_MAIN_TARGET})
    
    # Add test_csv_ops executable
    add_executable(test_csv_ops tests/test_csv_ops.c)
    target_link_libraries(test_csv_ops PRIVATE H5MR::h5mr ${CMPH_LIBRARIES} pthread)
    target_link_directories(test_csv_ops PRIVATE ${LOCAL_INCLUDE}/lib)
    target_include_directories(test_csv_ops PRIVATE ${CMPH_INCLUDE_DIRS})
    set_target_properties(test_csv_ops PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
    )
    add_test(NAME H5MR.test_csv_ops COMMAND test_csv_ops)
    add_dependencies(test_csv_ops ${H5MR_MAIN_TARGET})
    
    # Add test_h5m_create executable
    add_executable(test_h5m_create tests/test_h5m_create.c)
    target_link_libraries(test_h5m_create PRIVATE H5MR::h5mr ${CMPH_LIBRARIES})
    target_link_directories(test_h5m_create PRIVATE ${LOCAL_INCLUDE}/lib)
    target_include_directories(test_h5m_create PRIVATE ${CMPH_INCLUDE_DIRS})
    set_target_properties(test_h5m_create PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
    )
    if(H5MR_ENABLE_HEAVY_TESTS)
        add_test(NAME H5MR.test_h5m_create COMMAND test_h5m_create)
        set_tests_properties(H5MR.test_h5m_create PROPERTIES LABELS heavy)
    endif()
    add_dependencies(test_h5m_create ${H5MR_MAIN_TARGET} h5m-create)

    # Lightweight h5m-create regression (kept in default suite)
    add_executable(test_h5m_create_light tests/test_h5m_create.c)
    target_link_libraries(test_h5m_create_light PRIVATE H5MR::h5mr ${CMPH_LIBRARIES})
    target_link_directories(test_h5m_create_light PRIVATE ${LOCAL_INCLUDE}/lib)
    target_include_directories(test_h5m_create_light PRIVATE ${CMPH_INCLUDE_DIRS})
    target_compile_definitions(test_h5m_create_light PRIVATE
        H5MR_H5M_CREATE_LIGHT=1
        MIN_FILES_HISTORICAL=4
        MAX_FILES_HISTORICAL=6
        MIN_FILES_NEW=4
        MAX_FILES_NEW=6
        MIN_ROWS_PER_FILE=80
        MAX_ROWS_PER_FILE=120
        LARGE_FILES_COUNT=24
        LARGE_ROWS_PER_FILE=256
        H5MR_H5M_CREATE_MAX_SIZE_MB=600
    )
    set_target_properties(test_h5m_create_light PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
    )
    add_test(NAME H5MR.test_h5m_create_light COMMAND test_h5m_create_light)
    add_dependencies(test_h5m_create_light ${H5MR_MAIN_TARGET} h5m-create)

    # Convenience targets: light suite by default; heavy suite is opt-in
    add_custom_target(light_tests
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -LE heavy --build-config $<CONFIG> --test-dir ${CMAKE_BINARY_DIR}
        DEPENDS verify_layout test_meshid_ops test_h5mobaku_ops test_csv_ops test_h5m_create_light
        COMMENT "Run light tests (skips resource-intensive CSV/HDF5 conversion cases)"
    )
    if(H5MR_ENABLE_HEAVY_TESTS)
        add_custom_target(heavy_tests
            COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -L heavy --build-config $<CONFIG> --test-dir ${CMAKE_BINARY_DIR}
            DEPENDS test_csv_to_h5 test_h5m_create
            COMMENT "Run heavy conversion tests (CSV->HDF5 and h5m-create scenarios)"
        )
    endif()
endif()

# ───────────────────────────────────
#  CLI Tools (statically linked for portability)
# ───────────────────────────────────
option(H5MR_BUILD_CLI      "Build CLI tools"           ON)

if(H5MR_BUILD_CLI)
    # CLIツールは静的ライブラリとリンクしてポータブルに
    if(NOT H5MR_BUILD_STATIC)
        message(FATAL_ERROR "CLI tools require H5MR_BUILD_STATIC=ON")
    endif()

    add_executable(h5m-reader src/h5m-reader.c)
    target_link_libraries(h5m-reader PRIVATE
        h5mr_static
        ${HDF5_C_LIBRARIES}
        ${LIBURING_LIBRARIES}
        ${CMPH_LIBRARIES}
    )
    target_link_directories(h5m-reader PRIVATE ${LOCAL_INCLUDE}/lib)
    target_include_directories(h5m-reader PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMPH_INCLUDE_DIRS}
    )
    set_target_properties(h5m-reader PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
    )
    add_dependencies(h5m-reader h5mr_static)

    add_executable(h5m-create src/h5m-create.c)
    target_link_libraries(h5m-create PRIVATE
        h5mr_static
        ${HDF5_C_LIBRARIES}
        ${LIBURING_LIBRARIES}
        ${CMPH_LIBRARIES}
    )
    target_link_directories(h5m-create PRIVATE ${LOCAL_INCLUDE}/lib)
    target_include_directories(h5m-create PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMPH_INCLUDE_DIRS}
    )
    set_target_properties(h5m-create PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
    )
    add_dependencies(h5m-create h5mr_static)

    # Install CLI tools
    install(TARGETS h5m-reader h5m-create
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
