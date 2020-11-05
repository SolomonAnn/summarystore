package com.samsung.sra.experiments;

import com.samsung.sra.datastore.RationalPowerWindowing;
import com.samsung.sra.datastore.StreamException;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.aggregates.BloomFilterOperator;
import com.samsung.sra.datastore.aggregates.CMSOperator;
import com.samsung.sra.datastore.aggregates.MaxOperator;
import com.samsung.sra.datastore.aggregates.MinOperator;
import com.samsung.sra.datastore.aggregates.QuantileOperator;
import com.samsung.sra.datastore.aggregates.SimpleCountOperator;
import com.samsung.sra.datastore.aggregates.SumOperator;
import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import com.samsung.sra.datastore.storage.BackingStoreException;
import com.samsung.sra.experiments.iotdb.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class WriteREDDLowData {
	private static final Logger logger = LoggerFactory.getLogger(WriteREDDLowData.class);
	private static final String directory = "/data/tdstore_throughput_redd_low";
	private static final String prefix = "/data/redd/low_freq/house_";
	private static final String suffix = "/channel_1.dat";
	private static final int[] cycles = {100_000, 100_000, 100_000, 100_000, 500_000};
	private static final int threadsNum = 5;

	private static final String[] fileNames = {
		prefix + "1" + suffix, prefix + "2" + suffix, prefix + "3" + suffix,
		prefix + "4" + suffix, prefix + "5" + suffix,
	};

	public static void main(String[] args) throws IOException, InterruptedException {
		Semaphore parallelismSem = new Semaphore(100);
		Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + directory}).waitFor();

		try (SummaryStore store = new SummaryStore(directory, new SummaryStore.StoreOptions().setKeepReadIndexes(true))) {
			StreamWriter[] writers = new StreamWriter[threadsNum];
			Thread[] writerThreads = new Thread[threadsNum];
			for (int i = 0; i < threadsNum; i++) {
				writers[i] = new StreamWriter(store, parallelismSem, i, fileNames[i]);
				writerThreads[i] = new Thread(writers[i], i + "-appender");
			}
			long w0 = System.currentTimeMillis();
			for (int i = 0; i < threadsNum; ++i) {
				writerThreads[i].start();
			}
			for (int i = 0; i < threadsNum; ++i) {
				writerThreads[i].join();
			}
			long we = System.currentTimeMillis();
			logger.info("It costs {}min.", (we - w0) / 1000 / 60);
		} catch (BackingStoreException | ClassNotFoundException e) {
			logger.info(e.getMessage());
		}
	}

	private static class StreamWriter implements Runnable {
		private final long streamID;
		private final SummaryStore store;
		private final Semaphore semaphore;
		private final String fileName;

		private StreamWriter(SummaryStore store, Semaphore semaphore, long streamID, String fileName) {
			this.store = store;
			this.semaphore = semaphore;
			this.streamID = streamID;
			this.fileName = fileName;
		}

		@Override
		public void run() {
			if (semaphore != null) {
				semaphore.acquireUninterruptibly();
			}
			CountBasedWBMH wbmh = new CountBasedWBMH(new RationalPowerWindowing(1, 1, 1, 1))
				.setValuesAreLongs(true)
				.setBufferSize(800_000_000)
				.setWindowsPerMergeBatch(100_000)
				.setParallelizeMerge(100);
			try {
				store.registerStream(streamID, false, wbmh,
					new MaxOperator(),
					new MinOperator(),
					new SimpleCountOperator(),
					new SumOperator(),
					new CMSOperator(5, 1000, 0),
					new BloomFilterOperator(5, 1000),
					new QuantileOperator()
				);
				List<Long> time = new LinkedList<>();
				List<Float> value = new LinkedList<>();

				DataReader reader = new DataReader(fileName);
				List<String> data = reader.readData();

				for (String datum : data) {
					time.add(Long.parseLong(datum.split(" ")[0]));
					value.add(Float.parseFloat(datum.split(" ")[1]));
				}

				for (int i = 0; i < cycles[(int) streamID]; i++) {
					for (int j = 0; j < time.size(); j++) {
						store.append(streamID, time.get(j), value.get(j));
					}
					logger.info("streamID {} cycle {}", streamID, i);
					int len = time.size();
					long base = time.get(len - 1);
					time.clear();
					for (int j = 0; j < len; j++) {
						time.add(base + j + 1);
					}
				}
				logger.info("streamID {} time {}", streamID, time.get(time.size() - 1));
				wbmh.flushAndSetUnbuffered();
				logger.info("Populated stream {}", streamID);
				if (semaphore != null) {
					store.unloadStream(streamID);
					logger.info("Unloaded stream {}", streamID);
				}
			} catch (StreamException | BackingStoreException | IOException e) {
				logger.info(e.getMessage());
			} finally {
				if (semaphore != null) {
					semaphore.release();
				}
			}
		}
	}
}
