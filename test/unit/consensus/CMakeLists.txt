# Unit tests of the consensus module (src/consensus). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(consensus_tests DISCOVER
  SOURCES
    consensus_test.cpp
    consensus_persistence_test.cpp
  DEPS gvdb::consensus gvdb::cluster gvdb::core
  TIMEOUT 180 LABELS unit)
