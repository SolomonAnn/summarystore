package com.samsung.sra.experiments.iotdb;

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
import java.util.List;
import java.util.concurrent.Semaphore;

public class ExpandREDDHighFrequency {
    private static final Logger logger = LoggerFactory.getLogger(ExpandREDDHighFrequency.class);
    private static final String prefix = "/Users/anyanzhe/workspace/data/redd/high_freq/house_";
    private static final int pointNumPerWave = 275;
    private static final int threadsNum = 5;
    private static final String encoding = "GORILLA";
    private static final int[] cycles = {6, 6, 6, 30, 30};

    private static final String[] fileNames = {
        prefix + "3/current_1.dat", prefix + "3/current_2.dat", prefix + "3/voltage.dat",
        prefix + "5/current_1.dat", prefix + "5/current_2.dat",
    };

    public static void main(String[] args) throws IOException, InterruptedException, StorageEngineException {
        Semaphore parallelismSem = new Semaphore(100);
        IoTDBConfigCheck.getInstance().checkConfig();
        IoTDB store = IoTDB.getInstance();
        store.active();

        StreamWriter[] writers = new StreamWriter[threadsNum];
        Thread[] writerThreads = new Thread[threadsNum];
        for (int i = 0; i < threadsNum; i++) {
            writers[i] = new StreamWriter(store, parallelismSem, i, encoding, fileNames[i]);
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
        private final String encoding;
        private final String fileName;

        private StreamWriter(IoTDB store, Semaphore semaphore, long streamID, String encoding, String fileName) {
            this.store = store;
            this.semaphore = semaphore;
            this.streamID = streamID;
            this.encoding = encoding;
            this.fileName = fileName;
        }

        @Override
        public void run() {
            if (semaphore != null) {
                semaphore.acquireUninterruptibly();
            }
            try {
                DataReader reader = new DataReader(fileName);
                List<String> data = reader.readData();

                long[] time = new long[pointNumPerWave];
                long[] value = new long[pointNumPerWave];
                long[] intervals = new long[data.size() - 1];

                String storageGroupName = "root.group_" + streamID;
                String deviceName = "d";
                String sensorName = "s" + streamID;
                String dateType = "INT64";

                store.register(storageGroupName, deviceName, sensorName, dateType, encoding);

                long prev = Long.parseLong(data.get(0).split(" ")[0].replace(".", ""));
                for (int i = 1; i < data.size(); i++) {
                    long curr = Long.parseLong(data.get(i).split(" ")[0].replace(".", ""));
                    intervals[i - 1] = curr - prev;
                    prev = curr;
                }

                long base = 0;
                for (int c = 0; c < cycles[(int) streamID]; c++) {
                    for (int i = 0; i < intervals.length; i++) {
                        String[] points = data.get(i).split(" ");
                        long timestamp = base + Long.parseLong(points[0].replace(".", ""));
                        int cycle = Integer.parseInt(points[1].substring(0, points[1].indexOf('.')));
                        long interval = intervals[i] / cycle / pointNumPerWave;
                        for (int j = 0; j < cycle; j++) {
                            for (int k = 2; k < points.length; k++) {
                                time[k - 2] = timestamp + interval * ((long) j * pointNumPerWave + k - 2);
                                value[k - 2] = Long.parseLong(points[k].replace(".", ""));
                            }
                            if ((j + 1) % (49_500 / points.length) == 0) {
                                insertBatchWorker(storageGroupName, deviceName, sensorName, time, value);
                            }
                        }
                        insertBatchWorker(storageGroupName, deviceName, sensorName, time, value);
                        logger.info("streamID {} wave {}", streamID, i);
                    }
                    logger.info("streamID {} cycle {}", streamID, c);
                    base += Long.parseLong(data.get(data.size() - 1).split(" ")[0].replace(".", ""));
                }
            } catch (MetadataException | PathException | StorageGroupException | IOException | StorageEngineException e) {
                logger.info(e.getMessage());
            }
        }

        public void insertBatchWorker(String storageGroupName, String deviceName, String sensorName, long[] time, long[] value) {
            String[] measurements = {sensorName};
            List<Integer> dataTypes = new ArrayList<>();
            dataTypes.add(TSDataType.INT64.ordinal());
            BatchInsertPlan batchInsertPlan = new BatchInsertPlan(
                storageGroupName + "." + deviceName,
                measurements, dataTypes);

            Object[] columns = new Object[1];
            columns[0] = value;
            batchInsertPlan.setTimes(time);
            batchInsertPlan.setColumns(columns);
            batchInsertPlan.setRowCount(time.length);
            try {
                store.insertBatch(batchInsertPlan);
            } catch (StorageEngineException e) {
                logger.info(e.getMessage());
            }
        }
    }
}
