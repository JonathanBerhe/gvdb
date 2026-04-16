package io.gvdb.client;

import io.gvdb.client.model.CollectionInfo;
import io.gvdb.client.model.GvdbVector;
import io.gvdb.client.model.ImportStatus;
import io.gvdb.client.model.IndexType;
import io.gvdb.client.model.MetricType;
import io.gvdb.client.model.UpsertResult;
import io.gvdb.proto.BulkImportRequest;
import io.gvdb.proto.CancelImportRequest;
import io.gvdb.proto.CreateCollectionRequest;
import io.gvdb.proto.DropCollectionRequest;
import io.gvdb.proto.GetImportStatusRequest;
import io.gvdb.proto.GetRequest;
import io.gvdb.proto.HealthCheckRequest;
import io.gvdb.proto.HealthCheckResponse;
import io.gvdb.proto.ImportFormat;
import io.gvdb.proto.InsertRequest;
import io.gvdb.proto.InsertResponse;
import io.gvdb.proto.ListCollectionsRequest;
import io.gvdb.proto.ListVectorsRequest;
import io.gvdb.proto.UpsertRequest;
import io.gvdb.proto.VectorDBServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Synchronous GVDB client wrapping all gRPC RPCs.
 * <p>
 * Thread-safe. Uses a round-robin channel pool for write parallelism.
 * Protobuf types never leak — all methods use domain model types.
 */
public final class GvdbClient implements Closeable {

    private final GvdbClientConfig config;
    private final GvdbChannelPool pool;

    public GvdbClient(GvdbClientConfig config) {
        this.config = config;
        this.pool = new GvdbChannelPool(config);
    }

    public GvdbClient(String target) {
        this(GvdbClientConfig.builder(target).build());
    }

    // ---- Collection Management ----

    public CollectionInfo createCollection(String name, int dimension, MetricType metric, IndexType indexType) {
        var resp = stub().createCollection(CreateCollectionRequest.newBuilder()
                .setCollectionName(name)
                .setDimension(dimension)
                .setMetric(ProtoConverter.toProtoMetric(metric))
                .setIndexType(ProtoConverter.toProtoIndex(indexType))
                .build());
        return new CollectionInfo(name, resp.getCollectionId(), dimension, metric.name(), 0);
    }

    public void dropCollection(String name) {
        stub().dropCollection(DropCollectionRequest.newBuilder()
                .setCollectionName(name)
                .build());
    }

    public List<CollectionInfo> listCollections() {
        var resp = stub().listCollections(ListCollectionsRequest.getDefaultInstance());
        var result = new ArrayList<CollectionInfo>(resp.getCollectionsCount());
        for (var c : resp.getCollectionsList()) {
            result.add(ProtoConverter.fromProto(c));
        }
        return result;
    }

    // ---- Vector Operations ----

    public UpsertResult upsert(String collection, List<GvdbVector> vectors) {
        var resp = stub().upsert(UpsertRequest.newBuilder()
                .setCollectionName(collection)
                .addAllVectors(ProtoConverter.toProtoList(vectors))
                .build());
        return new UpsertResult(resp.getUpsertedCount(), resp.getInsertedCount(), resp.getUpdatedCount());
    }

    /**
     * Stream insert vectors in batches. Returns total number inserted.
     */
    public long streamInsert(String collection, Iterator<GvdbVector> vectors, int batchSize) {
        var totalInserted = new AtomicLong(0);
        var error = new AtomicReference<Throwable>();
        var latch = new CountDownLatch(1);

        var requestObserver = asyncStub().streamInsert(new StreamObserver<InsertResponse>() {
            @Override
            public void onNext(InsertResponse response) {
                totalInserted.set(response.getInsertedCount());
            }

            @Override
            public void onError(Throwable t) {
                error.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        var batchIterator = VectorBatcher.partitionLazy(vectors, batchSize);
        while (batchIterator.hasNext()) {
            var batch = batchIterator.next();
            requestObserver.onNext(InsertRequest.newBuilder()
                    .setCollectionName(collection)
                    .addAllVectors(ProtoConverter.toProtoList(batch))
                    .build());
        }
        requestObserver.onCompleted();

        try {
            if (!latch.await(config.timeout().toSeconds(), TimeUnit.SECONDS)) {
                throw new RuntimeException("StreamInsert timed out after " + config.timeout().toSeconds() + "s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("StreamInsert interrupted", e);
        }

        if (error.get() != null) {
            throw new RuntimeException("StreamInsert failed", error.get());
        }
        return totalInserted.get();
    }

    public List<GvdbVector> listVectors(String collection, int limit, long offset, boolean includeMetadata) {
        var resp = stub().listVectors(ListVectorsRequest.newBuilder()
                .setCollectionName(collection)
                .setLimit(limit)
                .setOffset(offset)
                .setIncludeMetadata(includeMetadata)
                .build());
        return ProtoConverter.fromProtoList(resp.getVectorsList());
    }

    public List<GvdbVector> get(String collection, List<Long> ids, boolean returnMetadata) {
        var resp = stub().get(GetRequest.newBuilder()
                .setCollectionName(collection)
                .addAllIds(ids)
                .setReturnMetadata(returnMetadata)
                .build());
        return ProtoConverter.fromProtoList(resp.getVectorsList());
    }

    // ---- Bulk Import (Server-Side) ----

    public String bulkImport(String collection, String sourceUri, String format,
                             String vectorColumn, String idColumn) {
        var fmt = "numpy".equalsIgnoreCase(format) ? ImportFormat.NUMPY : ImportFormat.PARQUET;
        var resp = stub().bulkImport(BulkImportRequest.newBuilder()
                .setCollectionName(collection)
                .setSourceUri(sourceUri)
                .setFormat(fmt)
                .setVectorColumn(vectorColumn)
                .setIdColumn(idColumn)
                .build());
        return resp.getImportId();
    }

    public ImportStatus getImportStatus(String importId) {
        var resp = stub().getImportStatus(GetImportStatusRequest.newBuilder()
                .setImportId(importId)
                .build());
        return ProtoConverter.fromProto(resp);
    }

    public void cancelImport(String importId) {
        stub().cancelImport(CancelImportRequest.newBuilder()
                .setImportId(importId)
                .build());
    }

    // ---- Health ----

    public void healthCheck() {
        var resp = stub().healthCheck(HealthCheckRequest.getDefaultInstance());
        if (resp.getStatus() != HealthCheckResponse.Status.SERVING) {
            throw new RuntimeException("Server not healthy: " + resp.getStatus() + " " + resp.getMessage());
        }
    }

    // ---- Lifecycle ----

    @Override
    public void close() {
        pool.close();
    }

    // ---- Internal ----

    private VectorDBServiceGrpc.VectorDBServiceBlockingStub stub() {
        return VectorDBServiceGrpc.newBlockingStub(pool.next())
                .withDeadlineAfter(config.timeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private VectorDBServiceGrpc.VectorDBServiceStub asyncStub() {
        return VectorDBServiceGrpc.newStub(pool.next())
                .withDeadlineAfter(config.timeout().toMillis(), TimeUnit.MILLISECONDS);
    }
}
