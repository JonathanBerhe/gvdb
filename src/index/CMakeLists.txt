# Index module: FAISS, TurboQuant, BM25 and sparse indexes, plus the optional
# CUDA and Metal backends.
gvdb_add_library(index
  SOURCES
    faiss/faiss_base.cpp
    faiss/faiss_flat.cpp
    faiss/faiss_hnsw.cpp
    faiss/faiss_ivf.cpp
    index_factory.cpp
    turboquant/turboquant_codec.cpp
    turboquant/turboquant_index.cpp
    turboquant/ivf_turboquant_index.cpp
    bm25_index.cpp
    sparse_index.cpp
  PUBLIC_DEPS
    gvdb::core
    # faiss/impl/HNSW.h includes <omp.h>, and the tests include the FAISS
    # wrappers in index/faiss.
    OpenMP::OpenMP_CXX
)

if(GVDB_WITH_CUDA)
  target_sources(gvdb_index PRIVATE cuda/cuda_compute.cpp)
  target_compile_definitions(gvdb_index PUBLIC GVDB_HAS_CUDA=1)
  # DO NOT simplify the LINK_GROUP / WHOLE_ARCHIVE expression. It works
  # around two linker quirks in upstream faiss v1.8.0:
  #
  #   1. libfaiss_gpu.a calls back into CPU symbols (faiss::ParameterSpace,
  #      faiss::IndexShardsIVF, ...). In the natural order `faiss faiss_gpu`
  #      the linker has already discarded faiss when faiss_gpu's references
  #      appear. LINK_GROUP RESCAN emits --start-group/--end-group so the
  #      linker scans the group again.
  #
  #   2. Upstream faiss attaches faiss_gpu with $<LINK_LIBRARY:WHOLE_ARCHIVE,...>
  #      on its own interface. Naming faiss_gpu in the group without the same
  #      feature makes CMake fail ("specified with the feature
  #      'WHOLE_ARCHIVE', has already occurred without any feature").
  target_link_libraries(gvdb_index
    PUBLIC
      "$<LINK_GROUP:RESCAN,faiss,$<LINK_LIBRARY:WHOLE_ARCHIVE,faiss_gpu>>"
    PRIVATE
      CUDA::cudart
      CUDA::cublas
  )
else()
  target_link_libraries(gvdb_index PUBLIC faiss)
endif()

if(GVDB_WITH_METAL)
  target_sources(gvdb_index
    PRIVATE
      metal/metal_compute.mm
      metal/metal_flat_index.mm
  )
  target_compile_definitions(gvdb_index PUBLIC GVDB_HAS_METAL=1)
  target_link_libraries(gvdb_index PRIVATE gvdb::metal_cpp)
  # The Metal backend compiles its kernel source at run time from
  # <build>/kernels.
  add_custom_command(TARGET gvdb_index POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      ${CMAKE_CURRENT_SOURCE_DIR}/metal/kernels/distance.metal
      ${PROJECT_BINARY_DIR}/kernels/distance.metal
  )
endif()
