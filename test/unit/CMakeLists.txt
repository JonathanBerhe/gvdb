# Unit tests, one folder per module of src/. Each folder builds one doctest
# binary, <module>_tests, and CTest runs each of its test cases as its own
# test, named "<module>_tests:<test case>".

add_subdirectory(core)
add_subdirectory(common)
add_subdirectory(config)
add_subdirectory(auth)
add_subdirectory(index)
add_subdirectory(storage)
add_subdirectory(compute)
add_subdirectory(cluster)
add_subdirectory(consensus)
add_subdirectory(rpc)
add_subdirectory(service)
add_subdirectory(server)
