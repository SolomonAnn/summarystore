/*
 * Copyright 2016 Samsung Research America. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.samsung.sra.experiments.iotdb;

import com.samsung.sra.experiments.Distribution;
import com.samsung.sra.experiments.ParetoDistribution;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.samsung.sra.experiments.PoissonDistribution;
import org.apache.iotdb.db.conf.IoTDBConfigCheck;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasureThroughput {

    private static final String directory = "/data/tdstore_throughput";
    private static final Logger logger = LoggerFactory.getLogger(MeasureThroughput.class);
    private static volatile boolean isEnd = false;

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err
                .println("SYNTAX: MeasureThroughputQuery numValuesPerThread numThreads batchSize encoding<PLAIN, DIFF, TS_2DIFF, RLE> isPossionDistribution<True:possion, False:pareto> queryIntervalInS [numParallelThreads]");
            System.exit(2);
        }
        long T = Long.parseLong(args[0].replace("_", ""));
        int nThreads = Integer.parseInt(args[1]);
        int batchSize = Integer.parseInt(args[2]);
        String encoding = args[3];
        boolean isPossionDIstri = false;
        if(args[4].equalsIgnoreCase("TRUE")){
            isPossionDIstri = true;
        }
        else if(args[4].equalsIgnoreCase("FALSE")){
            isPossionDIstri = false;
        }
        else {
            System.err.println("isPossionDistribution = "+isPossionDIstri+", which isn't a boolean variable.");
            System.err
                .println("SYNTAX: MeasureThroughputQuery numValuesPerThread numThreads batchSize encoding<PLAIN, DIFF, TS_2DIFF, RLE> isPossionDistribution<True:possion, False:pareto> [numParallelThreads]");
            System.exit(2);
        }

        long queryIntervalInMs = Integer.parseInt(args[5])*1000;

        Semaphore parallelismSem = args.length > 6
            ? new Semaphore(Integer.parseInt(args[6]))
            : null;
        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + directory}).waitFor();
        long st = System.currentTimeMillis();
        IoTDBConfigCheck.getInstance().checkConfig();
        IoTDB store = IoTDB.getInstance();
        store.active();

        StreamWriter[] writers = new StreamWriter[nThreads];
        Thread[] writerThreads = new Thread[nThreads];
        for (int i = 0; i < nThreads; ++i) {
            writers[i] = new StreamWriter(store, parallelismSem, i, T, batchSize, encoding, isPossionDIstri);
            writerThreads[i] = new Thread(writers[i], i + "-appender");
        }
        long w0 = System.currentTimeMillis();
        for (int i = 0; i < nThreads; ++i) {
            writerThreads[i].start();
        }

//        AtomicInteger threadCnt = new AtomicInteger();
//        ThreadPoolExecutor timedQueryThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(4,
//            r -> new Thread(r, "CompressThread-" + threadCnt.getAndIncrement()));
//
//        String[] aggreFunArray = {"sum", "count", "max_value", "min_value"};
//        Future<Pair<List<Long>, List<Double>>>[] queryFuture = new Future[aggreFunArray.length];
//        for(int i =0;i<aggreFunArray.length;i++){
//            final int finali = i;
//            queryFuture[i] = timedQueryThreadPool.submit(()->
//                timedQuery(store, queryIntervalInMs, aggreFunArray[finali], 3*30*86400*1000,7*86400*1000
//                ));
//        }

        for (int i = 0; i < nThreads; ++i) {
            writerThreads[i].join();
        }
//        isEnd = true;
//
//        for(int i = 0; i<aggreFunArray.length;i++){
//            Pair<List<Long>, List<Double>> queryResultPair = queryFuture[i].get();
//            StringBuilder latencyBuilder=new StringBuilder();
//            StringBuilder resultBuilder=new StringBuilder();
//            for(int j = 0;j<queryResultPair.right.size();j++){
//                latencyBuilder.append(queryResultPair.left.get(j)).append(",");
//                resultBuilder.append(queryResultPair.right.get(j)).append(",");
//            }
//
//            logger.info("[QUERY RESULT]-LATENCY for {} is {}",aggreFunArray[i], latencyBuilder.toString());
//            logger.info("[QUERY RESULT]-RESULT SET for {} is {}",aggreFunArray[i], resultBuilder.toString());
//        }

        long we = System.currentTimeMillis();
        logger.info("Write throughput = {} appends/s",
            String.format("%,.0f", (nThreads * T * 1000d / (we - w0))));
        Thread.sleep(5000);
        store.syncClose();
        logger.info("ALL TASK FINISH in {} min", (System.currentTimeMillis()-st)/1000/60.0);
        // logger.info("Stream 0 has {} elements in {} windows", T, store.getNumSummaryWindows(0L));

            /*long f0 = System.currentTimeMillis();
            store.query(0, 0, T - 1, 0);
            long fe = System.currentTimeMillis();
            logger.info("Time to run longest query, spanning [0, T) = {} sec", (fe - f0) / 1000d);*/

    }

    public static Pair<List<Long>, List<Double>> timedQuery(IoTDB store, long intervalTimeInMs, String aggreFun, long startTime, long queryLen)
        throws StorageEngineException {
        String deviceId = getStreamName(0);
        List<Long> latencys = new LinkedList<>();
        List<Double> result = new ArrayList<>();
        int cnt = 0;
        while (!isEnd) {
            long t0 = System.currentTimeMillis();
            double res = store.query(deviceId,aggreFun,startTime,startTime+queryLen);
            result.add(res);
            try {
                Thread.sleep(intervalTimeInMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long t1 = System.currentTimeMillis();
            latencys.add((t1-t0));
            cnt++;
            logger.info("[QUERY] {} {} query cost {}ms, result is {}",cnt,aggreFun, (t1-t0), res);
        }

        return new Pair(latencys, result);
    }

    private static String getStreamName(int streamID){
        String sensorName="s"+streamID;
        String deviceName="d";
        String groupName="root.group_"+streamID;
        return groupName+"."+deviceName+"."+sensorName;
    }


    private static class StreamWriter implements Runnable {

        private final long streamID, N;
        private final IoTDB store;
        private final Semaphore semaphore;
        private final SplittableRandom random;
        private final int batchSize;
        private final String encoding;
        private Distribution<Long> arrivalIntervalDistribution;

        private final int OUTPUT_UNIT = 100_000_000;

        private StreamWriter(IoTDB store, Semaphore semaphore, long streamID, long N, int batchSize, String encoding, boolean isPossion) {
            this.store = store;
            this.semaphore = semaphore;
            this.streamID = streamID;
            this.N = N;
            this.encoding = encoding;
            this.random = new SplittableRandom(streamID);
            this.batchSize = batchSize;
            if(isPossion){
                this.arrivalIntervalDistribution = new PoissonDistribution(10);
            }
            else{
                this.arrivalIntervalDistribution = new ParetoDistribution(1.0, 1.2);
            }

        }

        @Override
        public void run() {
            if (semaphore != null) {
                semaphore.acquireUninterruptibly();
            }
            try {
                String sensorName="s"+streamID;
                String deviceName="d";
                String groupName="root.group_"+streamID;
                String dateType = "INT64";
//        // PLAIN, PLAIN_DICTIONARY, RLE, DIFF, TS_2DIFF, BITMAP, GORILLA, REGULAR;
//        String encoding = "PLAIN";

                store.register(groupName, deviceName, sensorName, dateType, encoding);
                if(batchSize == 1){
                    insertWorker(groupName, deviceName, sensorName);
                }
                else {
                    insertBatchWorker(groupName, deviceName, sensorName, batchSize);
                }

                logger.info("Populated stream {}", streamID);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (semaphore != null) {
                    semaphore.release();
                }
            }
        }

        public void insertWorker(String groupName, String deviceName, String sensorName)
            throws QueryProcessException, StorageEngineException {
            long maxLatency = Long.MIN_VALUE;
            long minLatency = Long.MAX_VALUE;
            double avgLatency = 0;
            long currentTime = System.currentTimeMillis();
            // String deviceId, long insertTime, String measurement, String insertValue
            InsertPlan insertPlan = new InsertPlan(groupName+"."+deviceName, 0, sensorName, ""+0);
            String[] strVals = new String[1];
            long time = 0;
            for (long t = 0; t < N; ++t) {
                long v = random.nextInt(100);
                long startTime = System.nanoTime();
                time+=arrivalIntervalDistribution.next(random) + 1;
                insertPlan.setTime(time);
                strVals[0] = String.valueOf(v);
                insertPlan.setValues(strVals);
                store.insert(insertPlan);
                long endTime = System.nanoTime();
                long latency = endTime - startTime;
                maxLatency = Math.max(latency, maxLatency);
                minLatency = Math.min(latency, minLatency);
                avgLatency += latency;
                if ((t + 1) % OUTPUT_UNIT == 0) {
                    logger.info("Stream {} Batch {}: cost {}s, throughput {}points/s, max latency {}ns, " +
                            "min latency {}ns, avg latency {}ns", streamID, (t + 1) / OUTPUT_UNIT,
                        (System.currentTimeMillis() - currentTime) / 1000d,
                        Math.round(OUTPUT_UNIT / ((System.currentTimeMillis() - currentTime) / 1000d)),
                        maxLatency, minLatency, avgLatency/OUTPUT_UNIT );
                    maxLatency = Long.MIN_VALUE;
                    minLatency = Long.MAX_VALUE;
                    avgLatency = 0;
                    currentTime = System.currentTimeMillis();
                }
            }

        }


        public void insertBatchWorker(String groupName, String deviceName, String sensorName, int batchSize)
            throws StorageEngineException {

            long maxLatency = Long.MIN_VALUE;
            long minLatency = Long.MAX_VALUE;
            double avgLatency = 0;
            long currentTime = System.currentTimeMillis();


            String[] measurements = new String[1];
            measurements[0] = sensorName;

            List<Integer> dataTypes = new ArrayList<>();
            dataTypes.add(TSDataType.INT64.ordinal());

            BatchInsertPlan batchInsertPlan1 = new BatchInsertPlan(groupName+"."+deviceName, measurements,
                dataTypes);

            long[] times = new long[batchSize];
            Object[] columns = new Object[1];
            long[] values = new long[batchSize];


            long t = 0;
            long time = 0;
            while (t < N) {
                long startTime = System.currentTimeMillis();
                if(t+batchSize < N){
                    for(int k = 0; k < batchSize; k++){
                        time+=arrivalIntervalDistribution.next(random)+1;
                        times[k] = time;
                        values[k] = random.nextInt(100);
                        if (t % (N / 1_000L) == 0) {
                            logger.info("round " + t / (N / 1_000L) + " time " + time);
                        }
                        t++;
                    }
                }
                else{
                    int size = (int)(N-t);
                    times = new long[size];
                    values = new long[size];
                    for(int k = 0; k < size; k++){
                        time+=arrivalIntervalDistribution.next(random)+1;
                        times[k] = time;
                        values[k] = random.nextInt(100);
                        if (t % (N / 1_000L) == 0) {
                            logger.info("round " + t / (N / 1_000L) + " time " + time);
                        }
                        t++;
                    }
                }
                columns[0] = values;
                batchInsertPlan1.setTimes(times);
                batchInsertPlan1.setColumns(columns);
                batchInsertPlan1.setRowCount(times.length);
                store.insertBatch(batchInsertPlan1);
                long endTime = System.currentTimeMillis();
                long latency = endTime - startTime;
                maxLatency = Math.max(latency, maxLatency);
                minLatency = Math.min(latency, minLatency);
                avgLatency += latency;
                if ((t) % OUTPUT_UNIT == 0) {
                    logger.info("Stream {} Batch {}: cost {}s, throughput {}points/s, max latency {}ms, " +
                            "min latency {}ms, avg latency {}ms", streamID, (t) / OUTPUT_UNIT,
                        (System.currentTimeMillis() - currentTime) / 1000d,
                        Math.round(OUTPUT_UNIT / ((System.currentTimeMillis() - currentTime) / 1000d)),
                        maxLatency, minLatency, avgLatency / (OUTPUT_UNIT/batchSize) );
                    maxLatency = Long.MIN_VALUE;
                    minLatency = Long.MAX_VALUE;
                    avgLatency = 0;
                    currentTime = System.currentTimeMillis();
                }
            }

        }
    }
}
