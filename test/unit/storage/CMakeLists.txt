# Unit tests of the storage module (src/storage). One doctest binary.
# CTest runs each test case as its own test.
# AWS_EC2_METADATA_DISABLED keeps the AWS SDK from probing the EC2 metadata
# service when a test creates an S3 client off EC2.
gvdb_add_test(storage_tests DISCOVER
  SOURCES
    storage_test.cpp
    object_store_test.cpp
    segment_cache_test.cpp
    tiered_segment_manager_test.cpp
    segment_manifest_test.cpp
    bulk_import_test.cpp
    backup_manifest_test.cpp
    backup_manager_test.cpp
  DEPS gvdb::storage gvdb::index gvdb::config gvdb::common gvdb::core
  ENVIRONMENT AWS_EC2_METADATA_DISABLED=true
  TIMEOUT 120 LABELS unit)

# The object store contract against real cloud endpoints. Each binary exits
# with code 77 (reported as skipped) when its endpoint variable is not set:
# GVDB_TEST_S3_ENDPOINT (adobe/s3mock, test/integration/docker-compose.s3.yml) or
# GVDB_TEST_GCS_ENDPOINT (test/integration/docker-compose.fake-gcs.yml).
if(GVDB_WITH_S3)
  gvdb_add_test(object_store_s3_tests
    SOURCES object_store_s3_test.cpp
    DEPS gvdb::storage gvdb::core
    ENVIRONMENT AWS_EC2_METADATA_DISABLED=true
    CUSTOM_MAIN SKIP_RETURN_CODE 77
    TIMEOUT 120 LABELS integration s3)
endif()

if(GVDB_WITH_GCS)
  gvdb_add_test(object_store_gcs_tests
    SOURCES object_store_gcs_test.cpp
    DEPS gvdb::storage gvdb::core
    CUSTOM_MAIN SKIP_RETURN_CODE 77
    TIMEOUT 120 LABELS integration gcs)
endif()
