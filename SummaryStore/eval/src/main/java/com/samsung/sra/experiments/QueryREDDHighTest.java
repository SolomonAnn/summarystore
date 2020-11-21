package com.samsung.sra.experiments;

import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.StreamException;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class QueryREDDHighTest {
	private static final Logger logger = LoggerFactory.getLogger(QueryREDDHighTest.class);
	private static final Random random = new Random(11132313);

	private static final long N[] = {3919626153380631L, 3919625622616987L, 0L, 19603310575004711L, 19603310382523444L};

	private static int TOTAL_STREAM_NUM = 5;
	private static int QUERY_TIME = 100;
	private static int MONTH_OFFSET_LEN = 6;

	private static final String directory = "/data/tdstore_throughput_redd_high";

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
		QueryREDDHighTest queryTest = new QueryREDDHighTest();

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
					long streamID = random.nextInt(2);
					long endTime = N[(int)streamID] - queryOffsetLen1(offset) * offset.timeInUs;
					int range = random.nextInt((int) (offset.timeInUs * 0.05));
					endTime += random.nextDouble() > 0.5 ? range: -range;
					long startTime = endTime - queryLen.timeInUs;
					logger.info("stream = {}, aggreFun = {}, startTime = {}, endTime = {}, len = {}", streamID, aggreFun, startTime, endTime, queryLen.timeInUs);
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

	private static int queryOffsetLen1(TimeUnit offset){
		if(offset.equals(TimeUnit.HOUR)){
			return random.nextInt(24);
		} else if(offset.equals(TimeUnit.DAY)){
			return random.nextInt(7);
		} else if(offset.equals(TimeUnit.WEEK)){
			return random.nextInt(4);
		} else {
			return random.nextInt(4);
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
		HOUR(3_600_000_000L, "HOUR"),
		DAY(86_400_000_000L, "DAY"),
		WEEK(604_800_000_000L, "WEEK"),
		MONTH(2_592_000_000_000L, "MONTH");

		long timeInUs;
		String name;

		TimeUnit(long timeInUs, String name) {
			this.timeInUs = timeInUs;
			this.name = name;
		}
	}
}
