package com.samsung.sra.DataStore;

import org.teneighty.heap.FibonacciHeap;
import org.teneighty.heap.Heap;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Newer implementation of WBMH that maintains a priority queue of pending bucket
 * merge operations. Faster than the old implementation, but potentially uses more
 * memory, because it can end up building out the windowing very far into the future.
 * Can be controlled with care, which I've taken some of, but may need more analysis.
 */
public class CountBasedWBMH implements WindowingMechanism {
    private final WindowLengths windowLengths;

    private static class BucketInfo implements Serializable {
        BucketID prev = null, curr, next = null;
        long Cl, Cr;
        Heap.Entry<Long, BucketID> heapEntry = null;

        BucketInfo(BucketID prevBucket, BucketID thisBucket, BucketID nextBucket, long Cl, long Cr) {
            this.prev = prevBucket;
            this.curr = thisBucket;
            this.next = nextBucket;
            this.Cl = Cl;
            this.Cr = Cr;
        }
    }

    private final Map<BucketID, BucketInfo> bucketsInfo = new HashMap<>();
    /* Priority queue, mapping each BucketID b_i to the time at which b_{i+1} will be
     * merged into it. Using a Fibonacci heap instead of the Java Collections PriorityQueue
     * because we need an efficient arbitrary-element delete.
     *
     * Why this particular Fibonacci heap implementation?
     * https://gabormakrai.wordpress.com/2015/02/11/experimenting-with-dijkstras-algorithm/
     */
    private final FibonacciHeap<Long, BucketID> mergeCounts = new FibonacciHeap<>();

    public CountBasedWBMH(WindowLengths windowLengths) {
        this.windowLengths = windowLengths;
    }

    // maps window length to the start marker of the first window of that length
    private final TreeMap<Long, Long> firstWindowOfLength = new TreeMap<>();
    // all window start markers in an ordered set
    private final TreeSet<Long> windowStartMarkers = new TreeSet<>();

    private long lastWindowStart = 0L, lastLength = 0L;

    private void addWindow(long length) {
        assert length >= lastLength && length > 0;
        lastWindowStart += lastLength;
        if (length > lastLength) firstWindowOfLength.put(length, lastWindowStart);
        windowStartMarkers.add(lastWindowStart);
        lastLength = length;
    }

    /**
     * Add windows until we have one with length >= the specified target. Returns false
     * if the target length isn't achievable
     */
    private boolean addWindowsUntilLength(long targetLength) {
        if (targetLength <= lastLength) {
            // already added, nothing to do
            return true;
        } else {
            return windowLengths.addWindowsUntilLength(targetLength, this::addWindow);
        }
    }

    /**
     * Add windows until we have at least one window marker larger than the target
     */
    private void addWindowsPastMarker(long targetMarker) {
        while (lastWindowStart <= targetMarker) {
            addWindow(windowLengths.nextWindowLength());
        }
    }

    /**
     * Find the smallest N' >= N such that after N elements have been inserted
     * the interval [Cl, Cr] will be contained inside the same window. Returns
     * null if no such N' exists
     */
    private Long findMergeCount(long N, long Cl, long Cr) {
        assert 0 <= Cl && Cl <= Cr && Cr < N;
        long l = N-1 - Cr, r = N-1 - Cl, length = Cr - Cl + 1;

        if (!addWindowsUntilLength(length)) {
            return null;
        }
        long firstMarker = firstWindowOfLength.ceilingEntry(length).getValue();
        if (firstMarker >= l) {
            // l' == firstMarker, where l' := N'-1 - Cr
            return firstMarker + Cr + 1;
        } else {
            // we've already hit the target window length, so [l, r] are either
            // already in the same window or will be once they move into the next window
            addWindowsPastMarker(l);
            long currWindowL = windowStartMarkers.floor(l), currWindowR = windowStartMarkers.higher(l) - 1;
            if (r <= currWindowR) {
                // already in same window
                return N;
            } else {
                assert currWindowR - currWindowL + 1 >= length;
                // need to wait until next window, i.e. l' == currWindowR + 1, where l' := N'-1 - Cr
                return currWindowR + Cr + 2;
            }
        }
    }

    private BucketID lastBucketID = null;

    @Override
    public List<SummaryStore.BucketModification> computeModifications(
            TreeMap<BucketID, BucketMetadata> existingBuckets,
            long numValuesSoFar, Timestamp lastInsertedTimestamp,
            Timestamp newValueTimestamp, Object newValue) {
        if (lastInsertedTimestamp == null) assert numValuesSoFar == 0;

        BucketID newBucketID; // id of potential new bucket of size 1 holding the new (t, v) pair
        BucketMetadata newBucketMD;
        if (lastBucketID == null) {
            newBucketID = new BucketID(0);
            newBucketMD = new BucketMetadata(newBucketID, new Timestamp(0), 0L);
        } else {
            newBucketID = lastBucketID.nextBucketID();
            newBucketMD = new BucketMetadata(newBucketID, newValueTimestamp, numValuesSoFar);
        }


        long N = numValuesSoFar + 1;
        Map<BucketID, TreeSet<BucketID>> merges = new HashMap<>();
        // TODO: handle null findMergeCount
        while (!mergeCounts.isEmpty() && mergeCounts.getMinimum().getKey() <= N) {
            Heap.Entry<Long, BucketID> entry = mergeCounts.extractMinimum();
            BucketInfo b_i = bucketsInfo.get(entry.getValue());
            BucketInfo b_im1 = bucketsInfo.get(b_i.prev), b_ip1 = bucketsInfo.get(b_i.next);

            addMerge(merges, b_i.curr, b_i.next);
            // update linked list pointers: b_i's next, b_{i+2}'s prev
            b_i.next = b_ip1.next;
            if (b_ip1.next != null) {
                bucketsInfo.get(b_ip1.next).prev = b_i.curr;
            }
            // update b_i's Cr
            assert b_i.Cr == b_ip1.Cl - 1;
            b_i.Cr = b_ip1.Cr;

            // update heap keys: delete b_{i+1}, increase b_{i-1} and b_{i}. also delete b_{i+1} from bucketsInfo and mergeCounts
            if (b_im1 != null) {
                mergeCounts.delete(b_im1.heapEntry);
                b_im1.heapEntry = mergeCounts.insert(findMergeCount(N, b_im1.Cl, b_i.Cr), b_im1.curr);
            }
            if (b_ip1.heapEntry != null) {
                mergeCounts.delete(b_ip1.heapEntry);
            }
            bucketsInfo.remove(b_ip1.curr);
            if (lastBucketID == b_ip1.curr) {
                lastBucketID = b_i.curr;
            }
            b_ip1 = bucketsInfo.getOrDefault(b_i.next, null);
            if (b_ip1 != null) {
                b_i.heapEntry = mergeCounts.insert(findMergeCount(N, b_i.Cl, b_ip1.Cr), b_i.curr);
            }
        }

        if (N % 100_000 == 0) {
            System.out.println(
                    "[" + LocalDateTime.now() + "] N = " + N +
                    ": mergeCounts.size = " + mergeCounts.getSize() +
                    ", bucketsInfo.size = " + bucketsInfo.size() +
                    ", windowMap.size = " + firstWindowOfLength.size() +
                    ", windowSet.size = " + windowStartMarkers.size());
        }

        BucketInfo newBucketInfo = new BucketInfo(lastBucketID, newBucketID, null, newBucketMD.cStart, newBucketMD.cStart);
        // FIXME: assumes first window size is 1
        if (lastBucketID != null) {
            BucketInfo b_m1 = bucketsInfo.get(lastBucketID);
            b_m1.next = newBucketID;
            b_m1.heapEntry = mergeCounts.insert(findMergeCount(N, b_m1.Cl, newBucketInfo.Cr), lastBucketID);
        }
        lastBucketID = newBucketID;
        bucketsInfo.put(newBucketID, newBucketInfo);

        List<SummaryStore.BucketModification> bucketModifications = new ArrayList<>();
        for (Map.Entry<BucketID, TreeSet<BucketID>> merge: merges.entrySet()) {
            bucketModifications.add(new SummaryStore.BucketMergeModification(
                    merge.getKey(), new ArrayList<>(merge.getValue())));
        }
        bucketModifications.add(new SummaryStore.BucketCreateModification(newBucketMD));
        return bucketModifications;
    }

    private void addMerge(Map<BucketID, TreeSet<BucketID>> merges, BucketID dst, BucketID src) {
        if (!merges.containsKey(dst)) {
            merges.put(dst, new TreeSet<>());
        }
        // src needs to be merged into dst
        merges.get(dst).add(src);
        // any buckets that would have been merged into src now need to be merged into dst instead
        Set<BucketID> recursiveMerges = merges.remove(src);
        if (recursiveMerges != null) {
            for (BucketID bucketID: recursiveMerges) {
                merges.get(dst).add(bucketID);
            }
        }
    }}
