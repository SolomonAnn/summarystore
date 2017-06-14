package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.Utilities;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.integration.CacheLoader;
import org.cache2k.integration.CacheWriter;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RocksDBBackingStore extends BackingStore {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBBackingStore.class);

    private final RocksDB rocksDB;
    private final Options rocksDBOptions;
    private final WriteOptions rocksDBWriteOptions;
    private final ConcurrentMap<Long, SerDe> serdes = new ConcurrentHashMap<>();
    private final long totalCacheSize;
    private final Cache<SummaryKey, SummaryWindow> summaryCache;
    private final CacheLoader<SummaryKey, SummaryWindow> loader;
    private final CacheWriter<SummaryKey, SummaryWindow> writer;

    /**
     * @param rocksPath      on-disk path
     * @param totalCacheSize number of summary windows to cache in main memory. Set to 0 to disable caching
     * @throws BackingStoreException wrapping RocksDBException
     */
    public RocksDBBackingStore(String rocksPath, long totalCacheSize) throws BackingStoreException {
        totalCacheSize = totalCacheSize > 0 ? totalCacheSize : 1;
        this.totalCacheSize = totalCacheSize;
        rocksDBOptions = new Options()
                .setCreateIfMissing(true)
                .createStatistics()
                .setStatsDumpPeriodSec(300) // seconds
                .setMaxBackgroundCompactions(10) // number of threads
                .setAllowConcurrentMemtableWrite(true)
                .setMaxBytesForLevelBase(512L * 1024 * 1024)
                .setDbWriteBufferSize(4L * 1024 * 1024 * 1024)
                //.setCompressionType(CompressionType.NO_COMPRESSION)
                //.setMemTableConfig(new VectorMemTableConfig())
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setBlockCacheSize(8L * 1024 * 1024 * 1024)
                        .setFilter(new BloomFilter()))
                .setMaxOpenFiles(-1);
        rocksDBWriteOptions = new WriteOptions()
                .setDisableWAL(true);
        try {
            rocksDB = RocksDB.open(rocksDBOptions, rocksPath);
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
        loader = new CacheLoader<SummaryKey, SummaryWindow>() {
            @Override
            public SummaryWindow load(SummaryKey key) throws RocksDBException {
                return serdes.get(key.getStreamID()).deserialize(rocksDB.get(key.getBytes()));
            }
        };
        writer = new CacheWriter<SummaryKey, SummaryWindow>() {
            @Override
            public void write(SummaryKey summaryKey, SummaryWindow summaryWindow) throws RocksDBException {
                rocksDB.put(summaryKey.getBytes(), serdes.get(summaryKey.getStreamID()).serialize(summaryWindow));
            }

            @Override
            public void delete(SummaryKey summaryKey) throws Exception {
                rocksDB.delete(summaryKey.getBytes());
            }
        };
        if (totalCacheSize > 0) {
            summaryCache = new Cache2kBuilder<SummaryKey, SummaryWindow>() {
            }
                    .entryCapacity(totalCacheSize)
                    .loader(loader)
                    .writer(writer)
                    .build();
        } else {
            summaryCache = null;
        }
    }

    static {
        RocksDB.loadLibrary();
    }

    @Override
    void setSerDe(long streamID, SerDe serDe) {
        serdes.put(streamID, serDe);
    }

    private static class SummaryKey {
        private static final int SIZE = 16;

        private final byte[] bytes;

        private SummaryKey(long streamID, long windowID) {
            bytes = new byte[SIZE];
            Utilities.longToByteArray(streamID, bytes, 0);
            Utilities.longToByteArray(windowID, bytes, 8);
        }

        private SummaryKey(byte[] bytes) {
            assert bytes.length == SIZE;
            this.bytes = bytes;
        }

        private long getStreamID() {
            return Utilities.byteArrayToLong(bytes, 0);
        }

        private long getWindowID() {
            return Utilities.byteArrayToLong(bytes, 8);
        }

        private byte[] getBytes() {
            return bytes;
        }

        @Override
        public String toString() {
            return String.format("<%d, %d>", getStreamID(), getWindowID());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SummaryKey that = (SummaryKey) o;

            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    @Override
    SummaryWindow getSummaryWindow(StreamWindowManager windowManager, long swid) throws BackingStoreException {
        return summaryCache.get(new SummaryKey(windowManager.streamID, swid));
    }

    @Override
    SummaryWindow deleteSummaryWindow(StreamWindowManager windowManager, long swid) throws BackingStoreException {
        SummaryKey key = new SummaryKey(windowManager.streamID, swid);
        SummaryWindow val = summaryCache.get(key);
        summaryCache.remove(key);
        return val;
    }

    @Override
    void putSummaryWindow(StreamWindowManager windowManager, long swid, SummaryWindow window) throws BackingStoreException {
        summaryCache.put(new SummaryKey(windowManager.streamID, swid), window);
    }

    /*@Override
    public void warmupCache(Map<Long, StreamManager> streamManagers) throws RocksDBException {
        if (cache == null) return;

        RocksIterator iter = null;
        try {
            iter = rocksDB.newIterator();
            for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                byte[] keyArray = iter.key();
                if (keyArray.length != KEY_SIZE) continue; // ignore metadataSpecialKey
                long streamID = parseRocksDBKeyStreamID(keyArray);
                StreamManager streamManager = streamManagers.get(streamID);
                assert streamManager != null;
                ConcurrentHashMap<Long, SummaryWindow> streamCache = cache.get(streamID);
                if (streamCache == null) {
                    cache.put(streamID, streamCache = new ConcurrentHashMap<>());
                } else if (streamCache.size() >= totalCacheSize) {
                    continue;
                }
                long swid = parseRocksDBKeyWindowID(keyArray);
                SummaryWindow window = streamManager.deserializeSummaryWindow(iter.value());
                streamCache.put(swid, window);
            }
        } finally {
            if (iter != null) iter.dispose();
        }
    }*/

    @Override
    public void flushToDisk(StreamWindowManager windowManager) throws BackingStoreException {
        flushLandmarksToDisk(windowManager);
    }

    /* **** FIXME: Landmark cache has unbounded size **** */

    private static final int LANDMARK_KEY_SIZE = 17;

    private static byte[] getLandmarkRocksKey(long streamID, long lwid) {
        byte[] keyArray = new byte[LANDMARK_KEY_SIZE];
        keyArray[0] = 'L';
        Utilities.longToByteArray(streamID, keyArray, 1);
        Utilities.longToByteArray(lwid, keyArray, 9);
        return keyArray;
    }

    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, LandmarkWindow>> landmarkCache = new ConcurrentHashMap<>();

    private void flushLandmarksToDisk(StreamWindowManager windowManager) throws BackingStoreException {
        long streamID = windowManager.streamID;
        Map<Long, LandmarkWindow> streamMap = landmarkCache.get(streamID);
        if (streamMap == null) return;
        for (Map.Entry<Long, LandmarkWindow> windowEntry : streamMap.entrySet()) {
            long lwid = windowEntry.getKey();
            LandmarkWindow window = windowEntry.getValue();
            try {
                rocksDB.put(getLandmarkRocksKey(streamID, lwid), window.serialize());
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
        }
    }

    @Override
    LandmarkWindow getLandmarkWindow(StreamWindowManager windowManager, long lwid) throws BackingStoreException {
        Map<Long, LandmarkWindow> streamMap = landmarkCache.get(windowManager.streamID);
        if (streamMap != null && streamMap.containsKey(lwid)) {
            return streamMap.get(lwid);
        } else {
            byte[] bytes;
            try {
                bytes = rocksDB.get(getLandmarkRocksKey(windowManager.streamID, lwid));
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
            return LandmarkWindow.deserialize(bytes);
        }
    }

    @Override
    void putLandmarkWindow(StreamWindowManager windowManager, long lwid, LandmarkWindow window) {
        ConcurrentHashMap<Long, LandmarkWindow> stream = landmarkCache.get(windowManager.streamID);
        if (stream == null) {
            landmarkCache.put(windowManager.streamID, (stream = new ConcurrentHashMap<>()));
        }
        stream.put(lwid, window);
    }

    @Override
    void printWindowState(StreamWindowManager windowManager) throws BackingStoreException {
        System.out.println("stream " + windowManager.streamID + ":");
        System.out.println("\tuncached summary windows:");
        try (RocksIterator iter = rocksDB.newIterator()) {
            iter.seek(new SummaryKey(windowManager.streamID, 0L).getBytes());
            while (iter.isValid() && iter.key().length == SummaryKey.SIZE) {
                long streamID = new SummaryKey(iter.key()).getStreamID();
                if (streamID != windowManager.streamID) {
                    break;
                }
                System.out.println("\t\t" + serdes.get(streamID).deserialize(iter.value()));
                iter.next();
            }
            /*if (cache != null && cache.containsKey(windowManager.streamID)) {
                System.out.println("\tcached summary windows:");
                for (SummaryWindow window: cache.get(windowManager.streamID).values()) {
                    System.out.println("\t\t" + window);
                }
            }*/
            // TODO: landmarks
        }
    }

    @Override
    public void close() throws BackingStoreException {
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.close();
        logger.info("rocksDB closed");
    }
}
