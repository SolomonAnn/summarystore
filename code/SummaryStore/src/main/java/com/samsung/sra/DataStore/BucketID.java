package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.nio.ByteBuffer;

class BucketID implements Comparable<BucketID>, Serializable {
    private final int id;

    BucketID(int id) {
        this.id = id;
    }

    BucketID nextBucketID() {
        return new BucketID(id + 1);
    }

    /**
     * How many bytes long is a StreamID?
     */
    static final int byteCount = 4;

    /**
     * put id into buffer. Like all ByteBuffer puts, this advances the buffer position
     */
    void writeToByteBuffer(ByteBuffer buffer) {
        buffer.putInt(id);
    }

    /**
     * get id from buffer. Like all ByteBuffer gets, this advances the buffer position
     */
    static BucketID readFromByteBuffer(ByteBuffer buffer) {
        return new BucketID(buffer.getInt());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BucketID streamID = (BucketID) o;

        return id == streamID.id;

    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }

    @Override
    public int compareTo(BucketID that) {
        if (that == null) {
            throw new NullPointerException("comparing null BucketID");
        }
        return this.id - that.id;
    }
}
