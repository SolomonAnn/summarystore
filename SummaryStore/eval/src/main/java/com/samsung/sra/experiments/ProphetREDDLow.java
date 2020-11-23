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

	private static final long end[] = {158_010_967_022L};

	private static int[] stream = {0};
	private static long[][] offset = {
		{0, 100 * TimeUnit.YEAR.timeInSec, 1000 * TimeUnit.YEAR.timeInSec, 2000 * TimeUnit.YEAR.timeInSec, 3000 * TimeUnit.YEAR.timeInSec, 4000 * TimeUnit.YEAR.timeInSec},
	};
	private static long[] range = {
		37 * TimeUnit.DAY.timeInSec,
	};

	private static int QUERY_TIME = 1000;

	private static final String directory = "/data/tdstore_throughput_redd_low";

	public static void main(String[] args) throws BackingStoreException, IOException, ClassNotFoundException {
		if(args.length < 1) {
			System.err.println("SYNTAX: QUERY_NUM");
			System.exit(1);
		}

		QUERY_TIME = Integer.parseInt(args[0]);

		SummaryStore store = new SummaryStore(directory);
		ProphetREDDLow prophetREDDLow = new ProphetREDDLow();

		for (int j : stream) {
			prophetREDDLow.queryTest(store, j);
		}
	}

	public void queryTest(SummaryStore store, int streamID) {
		long[][] timestamp = new long [offset[streamID].length][QUERY_TIME];
		double[][] result = new double [offset[streamID].length][QUERY_TIME];

		for (int i = 0; i < offset[streamID].length; i++) {
			long endTime = end[streamID] - offset[streamID][i];
			long startTime = endTime - range[streamID];
			for (int j = 0; j < QUERY_TIME; j++) {
				try {
					ResultError sum = (ResultError) store.query(streamID, startTime + j * range[streamID] / QUERY_TIME, startTime + (j + 1) * range[streamID] / QUERY_TIME, 3);
					ResultError count = (ResultError) store.query(streamID, startTime + j * range[streamID] / QUERY_TIME, startTime + (j + 1) * range[streamID] / QUERY_TIME, 2);
					result[i][j] = Double.parseDouble(sum.result.toString()) / (Double.parseDouble(count.result.toString()) + 1);
				} catch (StreamException | BackingStoreException e) {
					logger.info(e.getMessage());
				}
				timestamp[i][j] = (startTime + j * range[streamID] / QUERY_TIME + startTime + (j + 1) * range[streamID] / QUERY_TIME) / 2;
			}
		}
		print(timestamp, result, streamID);
	}

	private void print(long[][] timestamp, double[][] result, int streamID){
		for (int i = 0; i < offset[streamID].length; i++) {
			logger.info("offset = {}", offset[streamID][i]);
			for (int j = 0; j < QUERY_TIME; j++) {
				System.out.println(timestamp[i][j] + "," + result[i][j]);
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
