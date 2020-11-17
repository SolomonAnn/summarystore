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

	private static final long start[] = {1302922280014060L, 1302922280014060L, 1303100823516613L, 1303100823516613L};
	private static final long end[] = {3919626153380631L, 3919625622616987L, 19603310575004711L, 19603310382523444L};

	private static int[] stream = {0, 1, 3, 4};
	private static int QUERY_TIME = 10000;

	private static final String directory = "/data/tdstore_throughput_redd_high";

	public static void main(String[] args) throws BackingStoreException, IOException, ClassNotFoundException {
		if(args.length < 1) {
			System.err.println("SYNTAX: QUERY_NUM");
			System.exit(1);
		}

		QUERY_TIME = Integer.parseInt(args[0]);

		SummaryStore store = new SummaryStore(directory);
		long st = System.currentTimeMillis();
		ProphetREDDHigh prophetREDDHigh = new ProphetREDDHigh();

		prophetREDDHigh.queryTest(store);

		logger.info("-QUERY-ALL TASK FINISH in {} min", (System.currentTimeMillis() - st) / 1000 / 60.0);
	}

	public void queryTest(SummaryStore store) {
		long st = System.currentTimeMillis();

		long[][] latency = new long [stream.length][QUERY_TIME];
		double[][] result = new double [stream.length][QUERY_TIME];

		for (int i = 0; i < stream.length; i++) {
			long interval = (end[i] - start[i]) / QUERY_TIME;
			for (int j = 0; j < QUERY_TIME; j++) {
				long streamID = stream[i];
				long startTime = start[i] + j * interval;
				long endTime = startTime + interval;
				logger.info("stream = {}, startTime = {}, endTime = {}", streamID, startTime, endTime);
				long t0 = System.currentTimeMillis();
				try {
					ResultError sum = (ResultError) store.query(streamID, startTime, endTime, 3);
					ResultError count = (ResultError) store.query(streamID, startTime, endTime, 2);
					result[i][j] = Double.parseDouble(sum.result.toString()) / Double.parseDouble(count.result.toString());
				} catch (StreamException | BackingStoreException e) {
					logger.info(e.getMessage());
				}
				long t1 = System.currentTimeMillis();
				latency[i][j] = t1 - t0;
			}
		}

		logger.info("-QUERY-Execute {} queries in {} s.", stream.length * QUERY_TIME, (System.currentTimeMillis() - st) / 1000);
		printLatency(latency);
		printQueryResult(result);
	}

	private void printLatency(long[][] latency) {
		logger.info("----------Latency----------");
		for (int i = 0; i < stream.length; i++) {
			logger.info("StreamID {}:", stream[i]);
			for (int j = 0; j < QUERY_TIME; j++) {
				logger.info("Time {} {}", j, latency[i][j]);
			}
		}
	}

	private void printQueryResult(double[][] result){
		logger.info("----------Result----------");
		for (int i = 0; i < stream.length; i++) {
			logger.info("StreamID {}:", stream[i]);
			for (int j = 0; j < QUERY_TIME; j++) {
				logger.info("Time {} {}", j, result[i][j]);
			}
		}
	}
}