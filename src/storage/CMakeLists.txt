# Storage module: segments, segment stores, object stores, bulk import and
# backup.
gvdb_add_library(storage
  SOURCES
    segment/batch_splitter.cpp
    segment/segment.cpp
    segment/segment_search.cpp
    segment/segment_io.cpp
    segment/segment_manager.cpp
    segment/scalar_index.cpp
    segment/segment_cache.cpp
    segment/segment_manifest.cpp
    segment/tiered_segment_manager.cpp
    segment/tiered_segment_sync.cpp
    objstore/object_store.cpp
    objstore/object_store_factory.cpp
    objstore/filesystem_object_store.cpp
    import/npy_reader.cpp
    import/bulk_importer.cpp
    backup/backup_manifest.cpp
    backup/segment_export.cpp
    backup/backup_manager.cpp
    backup/restore_manager.cpp
    backup/backup_util.cpp
  PUBLIC_DEPS
    gvdb::core
    gvdb::index
    gvdb::common
    absl::status
    absl::statusor
    absl::strings
  PRIVATE_DEPS
    absl::flat_hash_set
)

if(GVDB_WITH_S3)
  target_sources(gvdb_storage PRIVATE objstore/s3_object_store.cpp)
  target_link_libraries(gvdb_storage PUBLIC aws-cpp-sdk-s3 aws-cpp-sdk-core)
  target_compile_definitions(gvdb_storage PUBLIC GVDB_HAS_S3=1)
endif()

if(GVDB_WITH_GCS)
  target_sources(gvdb_storage PRIVATE objstore/gcs_object_store.cpp)
  target_link_libraries(gvdb_storage PUBLIC google-cloud-cpp::storage)
  target_compile_definitions(gvdb_storage PUBLIC GVDB_HAS_GCS=1)
endif()
