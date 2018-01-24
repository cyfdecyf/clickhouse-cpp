CMAKE_MINIMUM_REQUIRED(VERSION 3.0.2)

INCLUDE (cmake/cpp11.cmake)
INCLUDE (cmake/subdirs.cmake)

OPTION(BUILD_BENCHMARK "Build benchmark" OFF)
# Invoke cmake -DBUILD_PYBIND=ON -DPYTHON_EXECUTABLE:FILEPATH=/usr/bin/python2.7
# to build module for Python 2.7
OPTION(BUILD_PYBIND "Build Python binding" OFF)

PROJECT (CLICKHOUSE-CLIENT)

    USE_CXX11()

    IF (UNIX)
        IF (APPLE)
            SET (CMAKE_CXX_FLAGS "-O2 -Wall -Wextra -Werror")
        ELSE ()
            SET (CMAKE_CXX_FLAGS "-O2 -pthread -Wall -Wextra -Werror")
        ENDIF ()
        SET (CMAKE_EXE_LINKER_FLAGS, "-lpthread")
    ENDIF ()

    INCLUDE_DIRECTORIES(.)
    INCLUDE_DIRECTORIES(contrib)

    SUBDIRS (
        clickhouse
        contrib/cityhash
        contrib/gtest
        contrib/lz4
        tests/simple
        ut
    )

    IF (BUILD_BENCHMARK)
        SUBDIRS(bench)
    ENDIF (BUILD_BENCHMARK)

    IF (BUILD_PYBIND)
        SUBDIRS(pybind)
    ENDIF (BUILD_PYBIND)
