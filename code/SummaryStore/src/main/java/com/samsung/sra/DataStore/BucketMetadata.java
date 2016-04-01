package com.samsung.sra.DataStore;

import java.io.Serializable;

class BucketMetadata implements Serializable {
    final BucketID bucketID;
    // This Bucket covers elements in the time range [tStart, nextBucket.tStart), with insert
    // counts [cStart, nextBucket.cStart)
    final Timestamp tStart;
    final int cStart;
    final boolean isLandmark;

    BucketMetadata(BucketID bucketID, Timestamp tStart, int cStart, boolean isLandmark) {
        this.bucketID = bucketID;
        this.tStart = tStart;
        this.cStart = cStart;
        this.isLandmark = isLandmark;
    }

    BucketMetadata(BucketMetadata that) {
        this(that.bucketID, that.tStart, that.cStart, that.isLandmark);
    }

    @Override
    public String toString() {
        return "<bucket " + bucketID +
                ", " + (isLandmark ? "landmark" : "non-landmark") +
                ", tStart " + tStart +
                ", cStart " + cStart +
                ">";
    }
}