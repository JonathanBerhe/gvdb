# Storage module: segments, segment stores, object stores, bulk import and
# backup.
gvdb_add_library(storage
  SOURCES
    batch_splitter.cpp
    segment.cpp
    segment_manager.cpp
    scalar_index.cpp
    object_store.cpp
    object_store_factory.cpp
    filesystem_object_store.cpp
    segment_cache.cpp
    segment_manifest.cpp
    tiered_segment_manager.cpp
    npy_reader.cpp
    bulk_importer.cpp
    backup_manifest.cpp
    segment_export.cpp
    backup_manager.cpp
  PUBLIC_DEPS
    gvdb::core
    gvdb::index
    gvdb::utils
    absl::status
    absl::statusor
    absl::strings
)

if(GVDB_WITH_S3)
  target_sources(gvdb_storage PRIVATE s3_object_store.cpp)
  target_link_libraries(gvdb_storage PUBLIC aws-cpp-sdk-s3 aws-cpp-sdk-core)
  target_compile_definitions(gvdb_storage PUBLIC GVDB_HAS_S3=1)
endif()

if(GVDB_WITH_GCS)
  target_sources(gvdb_storage PRIVATE gcs_object_store.cpp)
  target_link_libraries(gvdb_storage PUBLIC google-cloud-cpp::storage)
  target_compile_definitions(gvdb_storage PUBLIC GVDB_HAS_GCS=1)
endif()

# Apache Arrow Parquet support (future, blocked on Arrow FetchContent and Docker issues)
# target_sources(gvdb_storage PRIVATE parquet_reader.cpp)
# target_link_libraries(gvdb_storage PUBLIC parquet_static)
