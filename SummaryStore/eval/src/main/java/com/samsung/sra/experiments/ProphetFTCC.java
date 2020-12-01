package com.samsung.sra.experiments;

import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.StreamException;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProphetFTCC {
	private static final Logger logger = LoggerFactory.getLogger(ProphetFTCC.class);

	private static final long end = 1605901835987878L;

	private static int streamID = 0;
	private static int QUERY_TIME = 10000;

	private static long[] offset = {0, TimeUnit.DAY.timeInUs, 2 * TimeUnit.WEEK.timeInUs, 4 * TimeUnit.WEEK.timeInUs};
	private static long range = 10 * TimeUnit.HOUR.timeInUs;

	private static final String directory = "/data/tdstore_throughput_ftcc";

	public static void main(String[] args) throws BackingStoreException, IOException, ClassNotFoundException {
		SummaryStore store = new SummaryStore(directory);
		ProphetFTCC prophetFTCC = new ProphetFTCC();
		prophetFTCC.queryTest(store);
	}

	public void queryTest(SummaryStore store) {
		long[][] timestamp = new long[offset.length][QUERY_TIME];
		double[][] result = new double[offset.length][QUERY_TIME];

		for (int i = 0; i < offset.length; i++) {
			long endTime = end - offset[i];
			long startTime = endTime - range;
			for (int j = 0; j < QUERY_TIME; j++) {
				try {
					ResultError sum = (ResultError) store.query(streamID, startTime + j * range / QUERY_TIME, startTime + (j + 1) * range / QUERY_TIME, 3);
					ResultError count = (ResultError) store.query(streamID, startTime + j * range / QUERY_TIME, startTime + (j + 1) * range / QUERY_TIME, 2);
					result[i][j] = Double.parseDouble(sum.result.toString()) / Double.parseDouble(count.result.toString());
				} catch (StreamException | BackingStoreException e) {
					logger.info(e.getMessage());
				}
				timestamp[i][j] = (startTime + j * range / QUERY_TIME + startTime + (j + 1) * range / QUERY_TIME) / 2;
			}
		}
		print(timestamp, result);
	}

	private void print(long[][] timestamp, double[][] result){
		for (int i = 0; i < offset.length; i++) {
			logger.info("offset = {}", i);
			for (int j = 0; j < QUERY_TIME; j++) {
				System.out.println(timestamp[i][j] + "," + result[i][j]);
			}
		}
	}

	enum TimeUnit{
		MINUTE(60_000_000L, "MIN"),
		HOUR(3_600_000_000L, "HOUR"),
		DAY(86_400_000_000L, "DAY"),
		WEEK(604_800_000_000L, "WEEK");

		long timeInUs;
		String name;

		TimeUnit(long timeInUs, String name) {
			this.timeInUs = timeInUs;
			this.name = name;
		}
	}
}
