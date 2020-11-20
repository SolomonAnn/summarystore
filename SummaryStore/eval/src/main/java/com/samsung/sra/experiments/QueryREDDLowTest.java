package com.samsung.sra.experiments;

import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.StreamException;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class QueryREDDLowTest {
	private static final Logger logger = LoggerFactory.getLogger(QueryREDDLowTest.class);
	private static final Random random = new Random(11132313);

	private static final long N[] = {158010967022L, 303951108756L, 194832201194L, 208575498345L, 948051387620L};

	private static int TOTAL_STREAM_NUM = 5;
	private static int QUERY_TIME = 100;
	private static int MONTH_OFFSET_LEN = 2;

	private static final String directory = "/data/tdstore_throughput_redd_low";

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
		QueryREDDLowTest queryTest = new QueryREDDLowTest();

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
				for(TimeUnit queryLen : TimeUnit.values()){
					long streamID = random.nextInt(TOTAL_STREAM_NUM);
					long endTime = N[(int)streamID] - queryOffsetLen(offset, (int)streamID) * offset.timeInSec;
					int range = random.nextInt((int) (offset.timeInSec * 0.05));
					endTime += random.nextDouble() > 0.5 ? range: -range;
					long startTime = endTime - queryLen.timeInSec;
					logger.info("stream = {}, aggreFun = {}, startTime = {}, endTime = {}, len = {}", streamID, aggreFun, startTime, endTime, queryLen.timeInSec);
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

	private static int queryOffsetLen(TimeUnit offset, int streamId){
		if(offset.equals(TimeUnit.YEAR)){
			return random.nextInt(10);
		} else if(offset.equals(TimeUnit.TENYEARS)){
			return random.nextInt(10);
		} else if(offset.equals(TimeUnit.HUNDREDYEARS)){
			return random.nextInt(10);
		} else {
			long maxTime = N[streamId];
			int range = (int)(maxTime / offset.timeInSec - 1);
			return random.nextInt(range);
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
	}

	enum TimeUnit{
		YEAR(31_536_000L, "YEAR"),
		TENYEARS(315_360_000L, "TENYEARS"),
		HUNDREDYEARS(3_153_600_000L, "HUNDREDYEARS"),
		THOUSANDYEARS(31_536_000_000L, "THOUSANDYEARS");

		long timeInSec;
		String name;

		TimeUnit(long timeInSec, String name) {
			this.timeInSec = timeInSec;
			this.name = name;
		}
	}
}
