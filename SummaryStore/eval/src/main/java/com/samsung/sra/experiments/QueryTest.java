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

import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.StreamException;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class QueryTest {
    private static final Logger logger = LoggerFactory.getLogger(QueryTest.class);
    private static final Random random = new Random(11132313);

    private static final long N[] = {31_250_000_000L, 31_250_000_000L, 31_250_000_000L,
                                     31_250_000_000L, 31_250_000_000L, 31_250_000_000L,
                                     31_250_000_000L, 31_250_000_000L, 31_250_000_000L,
                                     31_250_000_000L};

    private static int TOTAL_STREAM_NUM = 10;
    private static int QUERY_TIME = 5;
    private static int MONTH_OFFSET_LEN = 2;

    private static final String directory = "/data/tdstore_throughput";

    public static void main(String[] args) throws BackingStoreException, IOException, ClassNotFoundException {
        if(args.length < 3){
            System.err.println("SYNTAX: TOTAL_STREAM_NUM QUERY_NUM MONTH_OFFSET_LEN ");
            System.exit(2);
        }

        TOTAL_STREAM_NUM = Integer.parseInt(args[0]);
        QUERY_TIME = Integer.parseInt(args[1]);
        MONTH_OFFSET_LEN = Integer.parseInt(args[2]);

        SummaryStore store = new SummaryStore(directory);
        long st = System.currentTimeMillis();
        QueryTest queryTest = new QueryTest();

        queryTest.queryTest(store, 3, QUERY_TIME);
        queryTest.queryTest(store, 2, QUERY_TIME);
        queryTest.queryTest(store, 0, QUERY_TIME);
        queryTest.queryTest(store, 1, QUERY_TIME);

        logger.info("-QUERY-ALL TASK FINISH in {} min", (System.currentTimeMillis()-st)/1000/60.0);
    }

    public void queryTest(SummaryStore store, int aggreFun, int totalTimes) {
        long st = System.currentTimeMillis();

        // queryId, offset, queryLen
        long[][][] latency = new long [totalTimes][4][4];
        double[][][] result = new double [totalTimes][4][4];

        for(int i = 0; i< totalTimes; i++){
            for(TimeUnit offset: TimeUnit.values()){
                long streamID = random.nextInt(TOTAL_STREAM_NUM);
                long endTime = N[(int)streamID] - queryOffsetLen(offset) * offset.timeInMs;
                int range = random.nextInt((int) (offset.timeInMs * 0.05));
                endTime += random.nextDouble() > 0.5 ? range: -range;
                for(TimeUnit queryLen : TimeUnit.values()){
                    long startTime = endTime - queryLen.timeInMs;
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

    private static int queryOffsetLen(TimeUnit offset){
        if(offset.equals(TimeUnit.MINUTE)){
            return random.nextInt(60);
        } else if(offset.equals(TimeUnit.HOUR)){
            return random.nextInt(24);
        } else if(offset.equals(TimeUnit.DAY)){
            return random.nextInt(30);
        } else {
            return MONTH_OFFSET_LEN;
        }
    }

    private void printLatency(long[][][] latency){
        int totalTimes = latency.length;
        logger.info("-QUERY-****Latency in ms(row:offset, col:queryLen)***");
        for(int i = 0; i < totalTimes; i++){
            logger.info("-QUERY-Time {}:",i);
            for(TimeUnit offset: TimeUnit.values()){
                logger.info("-QUERY-,{},{},{},{},",
                    latency[i][offset.ordinal()][0],
                    latency[i][offset.ordinal()][1],
                    latency[i][offset.ordinal()][2],
                    latency[i][offset.ordinal()][3]
                );
            }
        }

        logger.info("-QUERY-Latency ---- Avg:");
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
            logger.info("-QUERY-,{},{},{},{},",
                minuteLenRes / totalTimes,
                hourLenRes / totalTimes,
                dayLenRes / totalTimes,
                monthLenRes / totalTimes
            );
        }
    }

    private void printQueryResult(double[][][] result){
        int totalTimes = result.length;
        logger.info("-QUERY-****Query Result(row:offset, col:queryLen)***");
        for(int i = 0; i < totalTimes; i++){
            logger.info("-QUERY-Time {}:", i);
            for(TimeUnit offset: TimeUnit.values()){
                logger.info("-QUERY-,{},{},{},{},",
                    result[i][offset.ordinal()][0],
                    result[i][offset.ordinal()][1],
                    result[i][offset.ordinal()][2],
                    result[i][offset.ordinal()][3]
                );
            }
        }

        logger.info("-QUERY-Query Result --- Avg:");
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
            logger.info("-QUERY-,{},{},{},{},",
                minuteLenRes / totalTimes,
                hourLenRes / totalTimes,
                dayLenRes / totalTimes,
                monthLenRes / totalTimes
            );
        }
    }

    enum TimeUnit{
        MINUTE(60_000L, "MIN"),
        HOUR(3_600_000L, "HOUR"),
        DAY(86_400_000L, "DAY"),
        MONTH(2_592_000_000L, "MON");

        long timeInMs;
        String name;

        TimeUnit(long timeInMs, String name) {
            this.timeInMs = timeInMs;
            this.name = name;
        }
    }
}
