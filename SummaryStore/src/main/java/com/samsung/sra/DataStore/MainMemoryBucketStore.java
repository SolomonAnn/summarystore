package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MainMemoryBucketStore implements BucketStore {
    private Map<Long, Map<Long, Bucket>> buckets = new HashMap<>();

    @Override
    public Bucket getBucket(StreamManager streamManager, long bucketID, boolean delete) throws RocksDBException {
        return delete ?
                buckets.get(streamManager.streamID).remove(bucketID) :
                buckets.get(streamManager.streamID).get(bucketID);
    }

    @Override
    public void putBucket(StreamManager streamManager, long bucketID, Bucket bucket) throws RocksDBException {
        Map<Long, Bucket> stream = buckets.get(streamManager.streamID);
        if (stream == null) {
            buckets.put(streamManager.streamID, (stream = new HashMap<>()));
        }
        stream.put(bucketID, bucket);
    }

    private Serializable indexes = null;

    @Override
    public Serializable getMetadata() throws RocksDBException {
        return indexes;
    }

    @Override
    public void putMetadata(Serializable indexes) throws RocksDBException {
        this.indexes = indexes;
    }

    @Override
    public void close() throws RocksDBException {
        buckets.clear();
        indexes = null;
    }
}