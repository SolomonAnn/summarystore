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
package com.samsung.sra.experiments;

import com.samsung.sra.datastore.RationalPowerWindowing;
import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.StreamException;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.aggregates.BloomFilterOperator;
import com.samsung.sra.datastore.aggregates.CMSOperator;
import com.samsung.sra.datastore.aggregates.MaxOperator;
import com.samsung.sra.datastore.aggregates.MinOperator;
import com.samsung.sra.datastore.aggregates.SimpleCountOperator;
import com.samsung.sra.datastore.aggregates.SumOperator;
import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import com.samsung.sra.datastore.storage.BackingStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SplittableRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class MeasureThroughput {
    private static final String directory = "/data/tdstore_throughput_pareto";
    private static final Logger logger = LoggerFactory.getLogger(MeasureThroughput.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("SYNTAX: MeasureThroughput numValuesPerThread numThreads [numParallelThreads]");
            System.exit(2);
        }
        long T = Long.parseLong(args[0].replace("_", ""));
        int nThreads = Integer.parseInt(args[1]);
        Semaphore parallelismSem = args.length > 2
                ? new Semaphore(Integer.parseInt(args[2]))
                : null;
        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + directory}).waitFor();

        try (SummaryStore store = new SummaryStore(directory, new SummaryStore.StoreOptions().setKeepReadIndexes(true))) {
            StreamWriter[] writers = new StreamWriter[nThreads];
            Thread[] writerThreads = new Thread[nThreads];
            for (int i = 0; i < nThreads; ++i) {
                writers[i] = new StreamWriter(store, parallelismSem, i, T);
                writerThreads[i] = new Thread(writers[i], i + "-appender");
            }
            long w0 = System.currentTimeMillis();
            for (int i = 0; i < nThreads; ++i) {
                writerThreads[i].start();
            }
            ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1);
            executor.scheduleAtFixedRate(() -> {
                try {
                    long startTime = System.currentTimeMillis();
                    ResultError resultError = (ResultError) store.query(8L, 233496979753L, 233497039753L, 3);
                    double result = Double.parseDouble(resultError.result.toString());
                    logger.info("func 3 latency {} ms result {}", System.currentTimeMillis() - startTime, result);
                    startTime = System.currentTimeMillis();
                    resultError = (ResultError) store.query(0L, 219674028248L, 219674088248L, 2);
                    result = Double.parseDouble(resultError.result.toString());
                    logger.info("func 2 latency {} ms result {}", System.currentTimeMillis() - startTime, result);
                } catch (StreamException | BackingStoreException e) {
                    logger.info(e.getMessage());
                }
            }, 0, 3, TimeUnit.MINUTES);
            for (int i = 0; i < nThreads; ++i) {
                writerThreads[i].join();
            }
            long we = System.currentTimeMillis();
            logger.info("Write throughput = {} appends/s",  String.format("%,.0f", (nThreads * T * 1000d / (we - w0))));
            store.loadStream(0L);
            logger.info("Stream 0 has {} elements in {} windows", T, store.getNumSummaryWindows(0L));

            executor.shutdown();
            /*long f0 = System.currentTimeMillis();
            store.query(0, 0, T - 1, 0);
            long fe = System.currentTimeMillis();
            logger.info("Time to run longest query, spanning [0, T) = {} sec", (fe - f0) / 1000d);*/
        }
    }

    private static class StreamWriter implements Runnable {
        private final long streamID, N;
        private final SummaryStore store;
        private final Semaphore semaphore;
        private final SplittableRandom splittableRandom;

        private StreamWriter(SummaryStore store, Semaphore semaphore, long streamID, long N) throws Exception {
            this.store = store;
            this.semaphore = semaphore;
            this.streamID = streamID;
            this.N = N;
            this.splittableRandom = new SplittableRandom(streamID);
        }

        @Override
        public void run() {
            if (semaphore != null) semaphore.acquireUninterruptibly();
            CountBasedWBMH wbmh = new CountBasedWBMH(new RationalPowerWindowing(1, 1, 1, 1))
                    .setValuesAreLongs(true)
                    .setBufferSize(800_000_000)
                    .setWindowsPerMergeBatch(100_000)
                    .setParallelizeMerge(100);
            try {
                store.registerStream(streamID, false, wbmh,
                        new MaxOperator(),
                        new MinOperator(),
                        new SimpleCountOperator(),
                        new SumOperator(),
                        new CMSOperator(5, 1000, 0),
                        new BloomFilterOperator(5, 1000));
                long maxLatency = Long.MIN_VALUE;
                long minLatency = Long.MAX_VALUE;
                double avgLatency = 0;
                long currentTime = System.currentTimeMillis();
                long startTime = System.currentTimeMillis();
                long queryTime = System.currentTimeMillis();
                long time = 0;
//                PoissonDistribution poissonDistribution = new PoissonDistribution(10);
                ParetoDistribution paretoDistribution = new ParetoDistribution(1.0, 1.2);
                for (long t = 0; t < N; ++t) {
                    if (System.currentTimeMillis() - queryTime > 180_000L && streamID == 0L) {
                        logger.info("stream ID " + streamID + " time " + time);
                        queryTime = System.currentTimeMillis();
                    }
//                    time += 1 + poissonDistribution.next(splittableRandom);
                    time += 1 + paretoDistribution.next(splittableRandom);
                    long v = splittableRandom.nextInt(100);
                    store.append(streamID, time, v);
                    if (System.currentTimeMillis() - queryTime > 180_000L && streamID == 0L) {
                        logger.info("stream ID " + streamID + " time " + time);
                        queryTime = System.currentTimeMillis();
                    }
                    if ((t + 1) % 50_000 == 0) {
                        maxLatency = Math.max(System.currentTimeMillis() - startTime, maxLatency);
                        minLatency = Math.min(System.currentTimeMillis() - startTime, minLatency);
                        avgLatency += System.currentTimeMillis() - startTime;
                        startTime = System.currentTimeMillis();
                    }
                    if ((t + 1) % 100_000_000 == 0) {
                        logger.info("Stream {} Batch {}: cost {}s, throughput {}points/s, max latency {}ms, " +
                                "min latency {}ms, avg latency {}ms", streamID, (t + 1) / 100_000_000,
                            (System.currentTimeMillis() - currentTime) / 1000d,
                            Math.round(100_000_000d / ((System.currentTimeMillis() - currentTime) / 1000d)),
                            maxLatency, minLatency, avgLatency / 2_000d);
                        maxLatency = Long.MIN_VALUE;
                        minLatency = Long.MAX_VALUE;
                        avgLatency = 0;
                        currentTime = System.currentTimeMillis();
                    }
                    if ((t + 1) % 400_000_000 == 0) {
                        store.flush(streamID);
                    }
                }
                logger.info("streamID {} time {}", streamID, time);
                /*store.flush(streamID);
                wbmh.setBufferSize(0);*/
                wbmh.flushAndSetUnbuffered();
                logger.info("Populated stream {}", streamID);
                if (semaphore != null) {
                    store.unloadStream(streamID);
                    logger.info("Unloaded stream {}", streamID);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (semaphore != null) semaphore.release();
            }
        }
    }
}
