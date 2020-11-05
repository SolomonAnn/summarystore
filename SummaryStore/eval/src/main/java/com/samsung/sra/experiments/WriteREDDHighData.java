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

public class WriteREDDHighData {
	private static final Logger logger = LoggerFactory.getLogger(WriteREDDHighData.class);
	private static final String directory = "/data/tdstore_throughput_redd_high";
	private static final String prefix = "/data/redd/high_freq/house_";
	private static final int pointNumPerWave = 275;
	private static final int threadsNum = 6;
	private static final int[] cycles = {3, 3, 3, 15, 15, 15};

	private static final String[] fileNames = {
		prefix + "3/current_1.dat", prefix + "3/current_2.dat", prefix + "3/voltage.dat",
		prefix + "5/current_1.dat", prefix + "5/current_2.dat", prefix + "5/voltage.dat",
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
					new QuantileOperator(50)
				);
				List<Long> intervals = new LinkedList<>();

				DataReader reader = new DataReader(fileName);
				List<String> data = reader.readData();

				long prev = Long.parseLong(data.get(0).split(" ")[0].replace(".", ""));
				for (int i = 1; i < data.size(); i++) {
					long curr = Long.parseLong(data.get(i).split(" ")[0].replace(".", ""));
					intervals.add(curr - prev);
					prev = curr;
				}

				long base = 0;
				long time = 0;
				float value;
				for (int c = 0; c < cycles[(int) streamID]; c++) {
					for (int i = 0; i < data.size() - 1; i++) {
						String[] points = data.get(i).split(" ");
						long timestamp = base + Long.parseLong(points[0].replace(".", ""));
						int cycle = Integer.parseInt(points[1].substring(0, points[1].indexOf('.')));
						long interval = intervals.get(i) / cycle / pointNumPerWave;
						for (int j = 0; j < cycle; j++) {
							for (int k = 2; k < points.length; k++) {
								time = timestamp + interval * ((long) j * pointNumPerWave + k - 2);
								value = Float.parseFloat(points[k]);
								store.append(streamID, time, value);
							}
						}
						logger.info("streamID {} wave {}", streamID, i);
					}
					logger.info("streamID {} cycle {}", streamID, c);
					base += Long.parseLong(data.get(data.size() - 1).split(" ")[0].replace(".", ""));
				}
				logger.info("streamID {} time {}", streamID, time);
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