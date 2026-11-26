# C++ code for the client API (vectordb.proto) and the node-to-node API
# (internal.proto).
gvdb_add_proto_library(gvdb_proto PROTOS vectordb.proto internal.proto)
add_library(gvdb::proto ALIAS gvdb_proto)
