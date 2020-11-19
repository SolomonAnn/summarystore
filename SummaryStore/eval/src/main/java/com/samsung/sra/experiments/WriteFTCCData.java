package com.samsung.sra.experiments;

import com.samsung.sra.datastore.RationalPowerWindowing;
import com.samsung.sra.datastore.StreamException;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.aggregates.BloomFilterOperator;
import com.samsung.sra.datastore.aggregates.CMSOperator;
import com.samsung.sra.datastore.aggregates.MaxOperator;
import com.samsung.sra.datastore.aggregates.MinOperator;
import com.samsung.sra.datastore.aggregates.SimpleCountOperator;
import com.samsung.sra.datastore.aggregates.SumOperator;
import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import com.samsung.sra.datastore.storage.BackingStoreException;
import com.samsung.sra.experiments.iotdb.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.concurrent.Semaphore;

public class WriteFTCCData {
	private static final Logger logger = LoggerFactory.getLogger(WriteFTCCData.class);
	private static final String directory = "/data/tdstore_throughput_ftcc";
	private static final String fileName = "/data/FTCC/FTPD-C919-10102-DH-201023-F-01-FTELPST-8192.txt";
	private static final int cycle = 500;
	private static final int threadsNum = 5;
	private static final String format = "yyyy-MM-dd HH:mm:ss:SSSSSS";

	public static void main(String[] args) throws IOException, InterruptedException {
		Semaphore parallelismSem = new Semaphore(100);
		Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + directory}).waitFor();

		try (SummaryStore store = new SummaryStore(directory, new SummaryStore.StoreOptions().setKeepReadIndexes(true))) {
			StreamWriter[] writers = new StreamWriter[threadsNum];
			Thread[] writerThreads = new Thread[threadsNum];
			for (int i = 0; i < threadsNum; i++) {
				writers[i] = new StreamWriter(store, parallelismSem, i);
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

		private StreamWriter(SummaryStore store, Semaphore semaphore, long streamID) {
			this.store = store;
			this.semaphore = semaphore;
			this.streamID = streamID;
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
					new BloomFilterOperator(5, 1000)
				);
				DataReader reader = new DataReader(fileName);
				List<String> data = reader.readData();
				data.remove(0);

				long[] time = new long[data.size()];
				long[] value = new long[data.size()];

				int cnt = 0;
				for (String point : data) {
					LocalDateTime localDateTime = LocalDateTime.parse(
						"2020-10-23 " + point.split("\\s+")[0],
						DateTimeFormatter.ofPattern(format)
					);
					time[cnt] = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli() * 1_000L + localDateTime.getLong(ChronoField.MICRO_OF_SECOND) % 1_000L;
					value[cnt] = Long.parseLong(point.split("\\s+")[1].replace(".", ""));
					cnt++;
				}

				for (int i = 0; i < cycle; i++) {
					for (int j = 0; j < time.length; j++) {
						store.append(streamID, time[j], value[(j + (int) streamID * data.size() / 5) % data.size()]);
					}
					logger.info("streamID {} cycle {}", streamID, i);
					int len = time.length;
					long base = time[time.length - 1] - time[0];
					for (int j = 0; j < len; j++) {
						time[j] += base + 122L;
					}
					if ((i + 1) % 100 == 0) {
						store.flush(streamID);
					}
				}
				logger.info("streamID {} time {}", streamID, time[time.length - 1]);
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
