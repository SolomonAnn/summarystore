package com.samsung.sra.experiments.iotdb;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import org.apache.iotdb.db.conf.IoTDBConfigCheck;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupException;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class ExpandREDDLowFrequency {
    private static final Logger logger = LoggerFactory.getLogger(ExpandREDDLowFrequency.class);
    private static final String prefix = "/data/redd/low_freq/house_";
    private static final String suffix = "/channel_1.dat";
    private static final int[] cycles = {10_000};
    private static final int threadsNum = 6;
    private static final int batchSize = 50_000;
    private static final String encoding = "GORILLA";

    private static final String[] fileNames = {
        prefix + "1" + suffix, prefix + "2" + suffix, prefix + "3" + suffix,
        prefix + "4" + suffix, prefix + "5" + suffix, prefix + "6" + suffix,
    };

    public static void main(String[] args) throws IOException, InterruptedException, StorageEngineException {
        Semaphore parallelismSem = new Semaphore(100);
        IoTDBConfigCheck.getInstance().checkConfig();
        IoTDB store = IoTDB.getInstance();
        store.active();

        StreamWriter[] writers = new StreamWriter[threadsNum];
        Thread[] writerThreads = new Thread[threadsNum];
        for (int i = 0; i < threadsNum; i++) {
            writers[i] = new StreamWriter(store, parallelismSem, i, batchSize, encoding, fileNames[i]);
            writerThreads[i] = new Thread(writers[i], i + "-appender");
        }
        for (int i = 0; i < threadsNum; ++i) {
            writerThreads[i].start();
        }
        for (int i = 0; i < threadsNum; ++i) {
            writerThreads[i].join();
        }
        Thread.sleep(5000);
        store.syncClose();
    }

    private static class StreamWriter implements Runnable {
        private final long streamID;
        private final IoTDB store;
        private final Semaphore semaphore;
        private final int batchSize;
        private final String encoding;
        private final String fileName;

        private StreamWriter(IoTDB store, Semaphore semaphore, long streamID, int batchSize,
                             String encoding, String fileName) {
            this.store = store;
            this.semaphore = semaphore;
            this.streamID = streamID;
            this.encoding = encoding;
            this.batchSize = batchSize;
            this.fileName = fileName;
        }

        @Override
        public void run() {
            if (semaphore != null) {
                semaphore.acquireUninterruptibly();
            }
            try {
                List<Long> time = new LinkedList<>();
                List<Float> value = new LinkedList<>();

                DataReader reader = new DataReader(fileName);
                List<String> data = reader.readData();

                String storageGroupName = "root.group_" + streamID;
                String deviceName = "d";
                String sensorName = "s" + streamID;
                String dateType = "FLOAT";

                store.register(storageGroupName, deviceName, sensorName, dateType, encoding);

                for (String datum : data) {
                    time.add(Long.parseLong(datum.split(" ")[0]));
                    value.add(Float.parseFloat(datum.split(" ")[1]));
                }

                List<Long> insertTime = time;
                for (int i = 0; i < cycles[(int) streamID]; i++) {
                    insertBatchWorker(storageGroupName, deviceName, sensorName, insertTime, value);
                    logger.info("streamID {} ts {}", streamID, i);
                    insertTime = time.stream().map(x -> x + data.size()).collect(Collectors.toList());
                }
            } catch (MetadataException | PathException | StorageGroupException | IOException | StorageEngineException e) {
                logger.info(e.getMessage());
            }
        }

        public void insertBatchWorker(String storageGroupName, String deviceName, String sensorName, List<Long> time, List<Float> value) {
            String[] measurements = {sensorName};
            List<Integer> dataTypes = new ArrayList<>();
            dataTypes.add(TSDataType.FLOAT.ordinal());
            BatchInsertPlan batchInsertPlan = new BatchInsertPlan(
                storageGroupName + "." + deviceName,
                measurements, dataTypes);

            int t = 0;
            while (t < time.size()) {
                int size = Math.min(time.size() - t, batchSize);
                long[] times = Longs.toArray(time.subList(t, t + size));
                for (long l : times) {
                    logger.info("streamID {} time {}", streamID, l);
                }
                Object[] columns = new Object[1];
                float[] values = Floats.toArray(value.subList(t, t + size));
                columns[0] = values;
                batchInsertPlan.setTimes(times);
                batchInsertPlan.setColumns(columns);
                batchInsertPlan.setRowCount(times.length);
                try {
                    store.insertBatch(batchInsertPlan);
                } catch (StorageEngineException e) {
                    logger.info(e.getMessage());
                }
                t += size;
            }
        }
    }
}
