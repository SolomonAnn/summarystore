package com.samsung.sra.datastore.ingest;

import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicInteger;

class FloatIngestBuffer implements IngestBuffer {
	static class FloatArray implements AutoCloseable {
		private final long ptr;

		private static Unsafe unsafe;

		static {
			try {
				Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
				unsafeConstructor.setAccessible(true);
				unsafe = unsafeConstructor.newInstance();
			} catch (ReflectiveOperationException e) {
				throw new RuntimeException(e);
			}
		}

		FloatArray(long capacity) {
			assert capacity >= 0;
			this.ptr = unsafe.allocateMemory(8 * capacity);
		}

		float get(long idx) {
			return unsafe.getFloat(ptr + 8 * idx);
		}

		void put(long idx, float val) {
			unsafe.putFloat(ptr + 8 * idx, val);
		}

		@Override
		public void close() {
			unsafe.freeMemory(ptr);
		}
	}

	// FIXME: must reconstruct on SummaryStore reopen
	private transient LongIngestBuffer.LongArray timestamps;
	private transient FloatArray values;
	private final int capacity;
	private int size = 0;
	private final int id;
	private static AtomicInteger num = new AtomicInteger(0);

	FloatIngestBuffer(int capacity) {
		this.capacity = capacity;
		this.id = num.incrementAndGet();

		this.timestamps = new LongIngestBuffer.LongArray(capacity);
		this.values = new FloatArray(capacity);
	}

	@Override
	public void append(long ts, Object value) {
		if (size >= capacity) throw new IndexOutOfBoundsException();
		timestamps.put(size, ts);
		values.put(size, ((Number) value).floatValue());
		++size;
	}

	@Override
	public boolean isFull() {
		return size == capacity;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public void truncateHead(int s) {
		assert s >= 0;
		if (s == 0) return;
		for (int i = 0; i < size - s; ++i) {
			timestamps.put(i, timestamps.get(i + s));
			values.put(i, values.get(i + s));
		}
		size -= s;
	}

	@Override
	public void clear() {
		size = 0;
	}

	@Override
	public long getTimestamp(int pos) {
		if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
		return timestamps.get(pos);
	}

	@Override
	public Object getValue(int pos) {
		if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
		return values.get(pos);
	}

	@Override
	public void close() {
		timestamps.close();
		values.close();
	}
}
