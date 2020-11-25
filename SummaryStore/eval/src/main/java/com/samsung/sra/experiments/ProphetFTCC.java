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

	private static final long start[] = {1605906499988109L, 1605822026988517L, 1604713939993869L, 1603521380058189L};
	private static final long end[] = {1605906804987854L, 1605822331988262L, 1604714244993614L, 1603521685057934L};

	private static int streamID = 0;
	private static int QUERY_TIME = 1000;

	private static final String directory = "/data/tdstore_throughput_ftcc";

	public static void main(String[] args) throws BackingStoreException, IOException, ClassNotFoundException {
		if(args.length < 1) {
			System.err.println("SYNTAX: QUERY_NUM");
			System.exit(1);
		}

		QUERY_TIME = Integer.parseInt(args[0]);

		SummaryStore store = new SummaryStore(directory);
		ProphetFTCC prophetFTCC = new ProphetFTCC();

		prophetFTCC.queryTest(store);
	}

	public void queryTest(SummaryStore store) {
		long[][] timestamp = new long[start.length][QUERY_TIME];
		double[][] result = new double[start.length][QUERY_TIME];

		for (int i = 0; i < start.length; i++) {
			long interval = (end[i] - start[i]) / QUERY_TIME;
			for (int j = 0; j < QUERY_TIME; j++) {
				long startTime = start[i] + j * interval;
				long endTime = startTime + interval;
				logger.info("stream = {}, startTime = {}, endTime = {}", streamID, startTime, endTime);
				try {
					ResultError sum = (ResultError) store.query(streamID, startTime, endTime, 3);
					ResultError count = (ResultError) store.query(streamID, startTime, endTime, 2);
					result[i][j] = Double.parseDouble(sum.result.toString()) / Double.parseDouble(count.result.toString());
				} catch (StreamException | BackingStoreException e) {
					logger.info(e.getMessage());
				}
				timestamp[i][j] = (startTime + endTime) / 2;
			}
		}

		print(timestamp, result);
	}

	private void print(long[][] timestamp, double[][] result){
		for (int i = 0; i < start.length; i++) {
			logger.info("offset = {}", i);
			for (int j = 0; j < QUERY_TIME; j++) {
				System.out.println(timestamp[i][j] + "," + result[i][j]);
			}
		}
	}
}
