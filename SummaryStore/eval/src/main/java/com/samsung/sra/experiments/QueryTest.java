package com.samsung.sra.experiments;

import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.StreamException;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class QueryTest {
    private static final Logger logger = LoggerFactory.getLogger(QueryTest.class);
    private final Random random = new Random(11132313);

    private static final long N = 31_250_000_000L;

    private static final int TOTAL_STREAM_NUM = 10;
    private static final int QUERY_TIME = 5;

    private static final String directory = "/data/tdstore_throughput";

    public static void main(String[] args) throws BackingStoreException, IOException, ClassNotFoundException {
        SummaryStore store = new SummaryStore(directory);
        long st = System.currentTimeMillis();
        QueryTest queryTest = new QueryTest();

        queryTest.queryTest(store, 3, QUERY_TIME);
        queryTest.queryTest(store, 2, QUERY_TIME);
        queryTest.queryTest(store, 0, QUERY_TIME);
        queryTest.queryTest(store, 1, QUERY_TIME);

        logger.info("ALL TASK FINISH in {} min", (System.currentTimeMillis() - st) / 1000 / 60.0);
    }

    public Pair<List<Long>, List<Double>> timedQuery(SummaryStore store, long intervalTimeInMs, int queryNum, int aggreFun, long startTime, TimeUnit queryLen){
        List<Long> latencys = new LinkedList<>();
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < queryNum; i++) {
            long t0 = System.currentTimeMillis();
            double res = 0;
            try {
                ResultError re = (ResultError) store.query(0L, startTime,startTime + queryLen.timeInMs, aggreFun);
                res = Double.parseDouble(re.result.toString());
            } catch (StreamException | BackingStoreException e) {
                logger.info(e.getMessage());
            }
            result.add(res);
            try {
                Thread.sleep(intervalTimeInMs);
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            }
            long t1 = System.currentTimeMillis();
            latencys.add((t1 - t0));
        }
        return new Pair(latencys, result);
    }

    public void queryTest(SummaryStore store, int aggreFun, int totalTimes) {
        long latestTime = N - 1;
        logger.info("Latest time in steram 0 is {}ms", latestTime);
        long st = System.currentTimeMillis();

        // queryId, offset, queryLen
        long[][][] latency = new long [totalTimes][4][4];
        double[][][] result = new double [totalTimes][4][4];

        for(int i = 0; i < totalTimes; i++){
            for(TimeUnit offset: TimeUnit.values()){
                long endTime = latestTime - offset.timeInMs;
                // 在offset的10%附近波动
                int range = random.nextInt((int) (offset.timeInMs * 0.05));
                endTime += random.nextDouble() > 0.5 ? range: -range;
                for(TimeUnit queryLen : TimeUnit.values()){
                    long startTime = endTime - queryLen.timeInMs;
                    long streamID = random.nextInt(TOTAL_STREAM_NUM);
                    logger.info("stream = {}, aggreFun = {}, startTime = {}, endTime = {}, len = {}", streamID, aggreFun, startTime, endTime, queryLen.timeInMs);
                    long t0 = System.currentTimeMillis();
                    try {
                        ResultError re = (ResultError) store.query(streamID, startTime, endTime, aggreFun);
                        result[i][offset.ordinal()][queryLen.ordinal()] = Double.parseDouble(re.result.toString());
                    } catch (StreamException | BackingStoreException e) {
                        logger.info(e.getMessage());
                    }
                    long t1 = System.currentTimeMillis();
                    latency[i][offset.ordinal()][queryLen.ordinal()] = t1 - t0;
                }
            }
        }
        logger.info("-QUERY-Execute {} {} queries in {} s.", totalTimes * 16, aggreFun, (System.currentTimeMillis() - st) / 1000);
        printLatency(latency);
        printQueryResult(result);
    }

    private void printLatency(long[][][] latency) {
        int totalTimes = latency.length;
        logger.info("****Latency in ms(row:offset, col:queryLen)***");
        for(int i = 0; i < totalTimes; i++) {
            logger.info("Time {}:",i);
            for(TimeUnit offset: TimeUnit.values()) {
                logger.info("{},{},{},{},",
                    latency[i][offset.ordinal()][0],
                    latency[i][offset.ordinal()][1],
                    latency[i][offset.ordinal()][2],
                    latency[i][offset.ordinal()][3]
                );
            }
        }

        logger.info("Latency ---- Avg:");
        for(TimeUnit offset: TimeUnit.values()){
            long minuteLenRes = 0;
            long hourLenRes = 0;
            long dayLenRes = 0;
            long monthLenRes = 0;
            for (long[][] longs : latency) {
                minuteLenRes += longs[offset.ordinal()][0];
                hourLenRes += longs[offset.ordinal()][1];
                dayLenRes += longs[offset.ordinal()][2];
                monthLenRes += longs[offset.ordinal()][3];
            }
            logger.info("{},{},{},{},",
                minuteLenRes/totalTimes,
                hourLenRes/totalTimes,
                dayLenRes/totalTimes,
                monthLenRes/totalTimes);
        }
    }

    private void printQueryResult(double[][][] result){
        int totalTimes = result.length;
        logger.info("****Query Result(row:offset, col:queryLen)***");
        for(int i = 0; i < totalTimes; i++){
            logger.info("Time {}:", i);
            for(TimeUnit offset: TimeUnit.values()){
                logger.info("{},{},{},{},",
                    result[i][offset.ordinal()][0],
                    result[i][offset.ordinal()][1],
                    result[i][offset.ordinal()][2],
                    result[i][offset.ordinal()][3]
                );
            }
        }

        logger.info("Query Result --- Avg:");
        for(TimeUnit offset: TimeUnit.values()){
            double minuteLenRes = 0;
            double hourLenRes = 0;
            double dayLenRes = 0;
            double monthLenRes = 0;
            for (double[][] doubles : result) {
                minuteLenRes += doubles[offset.ordinal()][0];
                hourLenRes += doubles[offset.ordinal()][1];
                dayLenRes += doubles[offset.ordinal()][2];
                monthLenRes += doubles[offset.ordinal()][3];
            }
            logger.info("{},{},{},{},",
                minuteLenRes/totalTimes,
                hourLenRes/totalTimes,
                dayLenRes/totalTimes,
                monthLenRes/totalTimes
            );
        }
    }

    enum TimeUnit{
        MINUTE(60_000L,"MIN"),
        HOUR(3_600_000L,"HOUR"),
        DAY(86_400_000L,"DAY"),
        MONTH(2_592_000_000L, "MON");

        long timeInMs;
        String name;

        TimeUnit(long timeInMs, String name) {
            this.timeInMs = timeInMs;
            this.name = name;
        }
    }
}
