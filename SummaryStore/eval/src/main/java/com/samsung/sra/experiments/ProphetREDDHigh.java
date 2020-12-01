package com.samsung.sra.experiments;

import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.StreamException;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProphetREDDHigh {
	private static final Logger logger = LoggerFactory.getLogger(ProphetREDDHigh.class);

	private static final long start = 1303100823516613L;
	private static final long end = 1359899045772143L;

	private static int streamID = 2;
	private static int QUERY_TIME = 10000;

	private static long[] offset = {(end - start) / 15, 8 * (end - start) / 15, end - start};
	private static long range = 1303101062985977L - 1303100823516613L;

	private static final String directory = "/data/tdstore_throughput_redd_high";

	public static void main(String[] args) throws BackingStoreException, IOException, ClassNotFoundException {
		SummaryStore store = new SummaryStore(directory);
		ProphetREDDHigh prophetREDDHigh = new ProphetREDDHigh();
		prophetREDDHigh.queryTest(store);
	}

	public void queryTest(SummaryStore store) {
		long[][] timestamp = new long[offset.length][QUERY_TIME];
		double[][] result = new double[offset.length][QUERY_TIME];

		for (int i = 0; i < offset.length; i++) {
			long startTime = end - offset[i];
			for (int j = 0; j < QUERY_TIME; j++) {
				try {
					ResultError sum = (ResultError) store.query(streamID, startTime + j * range / QUERY_TIME, startTime + (j + 1) * range / QUERY_TIME, 3);
					ResultError count = (ResultError) store.query(streamID, startTime + j * range / QUERY_TIME, startTime + (j + 1) * range / QUERY_TIME, 2);
					result[i][j] = Double.parseDouble(sum.result.toString()) / (Double.parseDouble(count.result.toString()) + 1);
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
			logger.info("offset = {}", offset[i]);
			for (int j = 0; j < QUERY_TIME; j++) {
				System.out.println(timestamp[i][j] + "," + result[i][j]);
			}
		}
	}
}
