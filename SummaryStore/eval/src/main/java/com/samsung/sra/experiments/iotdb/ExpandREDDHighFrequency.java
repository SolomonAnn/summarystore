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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class ExpandREDDHighFrequency {
    private static final Logger logger = LoggerFactory.getLogger(ExpandREDDHighFrequency.class);
    private static final String directory = "/data/tdstore_throughput";
    private static final String prefix = "/data/redd/high_freq/house_";
    private static final int pointNumPerWave = 275;
    private static final int threadsNum = 6;
    private static final int batchSize = 50_000;
    private static final String encoding = "GORILLA";

    private static final String[] fileNames = {
        prefix + "3/current_1.dat", prefix + "3/current_2.dat", prefix + "3/voltage.dat",
        prefix + "5/current_1.dat", prefix + "5/current_2.dat", prefix + "5/voltage.dat",
    };

    public static void main(String[] args) throws IOException, InterruptedException, StorageEngineException {
        Semaphore parallelismSem = new Semaphore(100);
        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + directory}).waitFor();
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
                List<Long> intervals = new LinkedList<>();

                DataReader reader = new DataReader(fileName);
                List<String> data = reader.readData();

                long prev = Long.parseLong(data.get(0).split(" ")[0].replace(".", ""));
                for (int i = 1; i < data.size() - 1; i++) {
                    long curr = Long.parseLong(data.get(i).split(" ")[0].replace(".", ""));
                    intervals.add(curr - prev);
                    prev = curr;
                }

                for (int i = 0; i < data.size() - 1; i++) {
                    String[] points = data.get(i).split(" ");
                    long timestamp = Long.parseLong(points[0].replace(".", ""));
                    int cycle = Integer.parseInt(points[1].substring(0, points[1].indexOf('.')));
                    long interval = intervals.get(i) / cycle / pointNumPerWave;
                    for (int j = 0; j < cycle; j++) {
                        for (int k = 2; k < points.length; k++) {
                            time.add(timestamp + interval * ((long) j * pointNumPerWave + k - 2));
                            value.add(Float.parseFloat(points[k]));
                        }
                    }
                    logger.info("streamID {} ts {}", streamID, i);
                }

                String storageGroupName = "root.group_" + streamID;
                String deviceName = "d";
                String sensorName = "s" + streamID;
                String dateType = "FLOAT";

                store.register(storageGroupName, deviceName, sensorName, dateType, encoding);
                insertBatchWorker(storageGroupName, deviceName, sensorName, time, value);
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
                long[] times = new long[size];
                Object[] columns = new Object[1];
                float[] values = new float[size];
                for(int k = 0; k < size; k++){
                    times[k] = time.get(t);
                    values[k] = value.get(t);
                    t++;
                }
                columns[0] = values;
                batchInsertPlan.setTimes(times);
                batchInsertPlan.setColumns(columns);
                batchInsertPlan.setRowCount(times.length);
                try {
                    store.insertBatch(batchInsertPlan);
                } catch (StorageEngineException e) {
                    logger.info(e.getMessage());
                }
                if (t % 100_000_000 == 0) {
                    logger.info("streamID {} pt {}", streamID, t);
                }
            }
        }
    }
}
