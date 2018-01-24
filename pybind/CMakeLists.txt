ADD_SUBDIRECTORY(pybind11)

pybind11_add_module(_clickhouse
    clickhouse.cpp)

TARGET_LINK_LIBRARIES(_clickhouse
    PRIVATE clickhouse-cpp-lib)

# Copy python file to build directory to ease test.
CONFIGURE_FILE(
    clickhouse.py
    ${CMAKE_CURRENT_BINARY_DIR}/clickhouse.py
    COPYONLY)
CONFIGURE_FILE(
    test_clickhouse.py
    ${CMAKE_CURRENT_BINARY_DIR}/test_clickhouse.py
    COPYONLY)
