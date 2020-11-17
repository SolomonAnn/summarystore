package com.samsung.sra.experiments;

import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.StreamException;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProphetREDDLow {
	private static final Logger logger = LoggerFactory.getLogger(ProphetREDDLow.class);

	private static final long start[] = {1303132929L, 1303082307L, 1302930687L, 1303002979L, 1303100643L};
	private static final long end[] = {79389267022L, 121159508756L, 72671001194L, 85299098345L, 76837387620L};

	private static int[] stream = {0, 1, 2, 3, 4};
	private static long[][] offset = {
		{1000 * TimeUnit.YEAR.timeInSec, 2000 * TimeUnit.YEAR.timeInSec, 0},
		{1000 * TimeUnit.YEAR.timeInSec, 2000 * TimeUnit.YEAR.timeInSec, 3000 * TimeUnit.YEAR.timeInSec, 0},
		{1000 * TimeUnit.YEAR.timeInSec, 2000 * TimeUnit.YEAR.timeInSec, 0},
		{1000 * TimeUnit.YEAR.timeInSec, 2000 * TimeUnit.YEAR.timeInSec, 0},
		{1000 * TimeUnit.YEAR.timeInSec, 2000 * TimeUnit.YEAR.timeInSec, 0}
	};
	private static long[] range = {
		370 * TimeUnit.DAY.timeInSec,
		350 * TimeUnit.DAY.timeInSec,
		450 * TimeUnit.DAY.timeInSec,
		480 * TimeUnit.DAY.timeInSec,
		440 * TimeUnit.DAY.timeInSec
	};

	private static int QUERY_TIME = 10000;

	private static final String directory = "/data/tdstore_throughput_redd_low";

	public static void main(String[] args) throws BackingStoreException, IOException, ClassNotFoundException {
		if(args.length < 1) {
			System.err.println("SYNTAX: QUERY_NUM");
			System.exit(1);
		}

		QUERY_TIME = Integer.parseInt(args[0]);

		SummaryStore store = new SummaryStore(directory);
		long st = System.currentTimeMillis();
		ProphetREDDLow prophetREDDLow = new ProphetREDDLow();

		for (int i = 0; i < stream.length; i++) {
			logger.info("streamID {}", stream[i]);
			prophetREDDLow.queryTest(store, stream[i]);
		}

		logger.info("-QUERY-ALL TASK FINISH in {} min", (System.currentTimeMillis() - st) / 1000 / 60.0);
	}

	public void queryTest(SummaryStore store, int streamID) {
		long st = System.currentTimeMillis();

		long[][] latency = new long [offset[streamID].length][QUERY_TIME];
		double[][] result = new double [offset[streamID].length][QUERY_TIME];

		for (int i = 0; i < offset[streamID].length; i++) {
			long endTime = end[streamID] - offset[streamID][i];
			long startTime = endTime - range[streamID];
			for (int j = 0; j < QUERY_TIME; j++) {
				logger.info("stream = {}, startTime = {}, endTime = {}", streamID, startTime, endTime);
				long t0 = System.currentTimeMillis();
				try {
					ResultError sum = (ResultError) store.query(streamID, startTime + j * range[streamID] / QUERY_TIME, startTime + (j + 1) * range[streamID] / QUERY_TIME, 3);
					ResultError count = (ResultError) store.query(streamID, startTime + j * range[streamID] / QUERY_TIME, startTime + (j + 1) * range[streamID] / QUERY_TIME, 2);
					result[i][j] = Double.parseDouble(sum.result.toString()) / Double.parseDouble(count.result.toString());
				} catch (StreamException | BackingStoreException e) {
					logger.info(e.getMessage());
				}
				long t1 = System.currentTimeMillis();
				latency[i][j] = t1 - t0;
			}
		}

		logger.info("-QUERY-Execute {} queries in {} s.", stream.length * stream.length, (System.currentTimeMillis() - st) / 1000);
		printLatency(latency, streamID);
		printQueryResult(result, streamID);
	}

	private void printLatency(long[][] latency, int streamID) {
		logger.info("----------Latency----------");
		for (int i = 0; i < offset[streamID].length; i++) {
			logger.info("StreamID {}:", stream[i]);
			for (int j = 0; j < QUERY_TIME; j++) {
				logger.info("Time {} {}", j, latency[i][j]);
			}
		}
	}

	private void printQueryResult(double[][] result, int streamID){
		logger.info("----------Result----------");
		for (int i = 0; i < offset[streamID].length; i++) {
			logger.info("StreamID {}:", stream[i]);
			for (int j = 0; j < QUERY_TIME; j++) {
				logger.info("Time {} {}", j, result[i][j]);
			}
		}
	}

	enum TimeUnit{
		MINUTE(60L, "MIN"),
		HOUR(3_600L, "HOUR"),
		DAY(86_400L, "DAY"),
		MONTH(2_592_000L, "MON"),
		YEAR(31_536_000L, "YEAR");

		long timeInSec;
		String name;

		TimeUnit(long timeInSec, String name) {
			this.timeInSec = timeInSec;
			this.name = name;
		}
	}
}
