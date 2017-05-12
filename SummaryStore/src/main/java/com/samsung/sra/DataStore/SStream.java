package com.samsung.sra.DataStore;

import com.samsung.sra.DataStore.Storage.BackingStore;
import com.samsung.sra.DataStore.Storage.BackingStoreException;
import com.samsung.sra.DataStore.Storage.StreamWindowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;


/**
 * One Summary Store stream. Called SStream instead of Stream to differentiate from java.util.Stream
 */
class SStream implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(SStream.class);

    private final long streamID;
    private final WindowOperator[] operators;

    final StreamWindowManager windowManager;

    final StreamStatistics stats;
    private long tLastAppend = -1, tLastLandmarkStart = -1, tLastLandmarkEnd = -1;
    private long activeLWID = -1; // id of active landmark window
    private long nextLWID = 0; // id of next landmark window to be created

    final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * WindowingMechanism object. Maintains write indexes internally, which SummaryStore
     * will persist to disk along with the rest of SStream
     */
    final WindowingMechanism windowingMechanism;

    void populateTransientFields(BackingStore backingStore, ExecutorService executorService) {
        windowManager.populateTransientFields(backingStore);
        windowingMechanism.populateTransientFields(executorService);
    }

    SStream(long streamID, WindowingMechanism windowingMechanism, WindowOperator... operators) {
        this.streamID = streamID;
        this.operators = operators;
        this.windowingMechanism = windowingMechanism;
        windowManager = new StreamWindowManager(streamID, operators);
        stats = new StreamStatistics();
    }

    void append(long ts, Object[] value) throws BackingStoreException, StreamException {
        if (ts <= tLastAppend || ts < tLastLandmarkStart || ts <= tLastLandmarkEnd) {
            throw new StreamException(String.format("out-of-order insert in stream %d: ts = %d", streamID, ts));
        }
        tLastAppend = ts;
        stats.append(ts, value);
        if (activeLWID == -1) {
            // insert into decayed window sequence
            windowingMechanism.append(windowManager, ts, value);
        } else {
            // update decayed windowing, aging it by one position, but don't actually insert value into decayed window;
            // see how LANDMARK_SENTINEL is handled in StreamWindowManager.insertIntoSummaryWindow
            windowingMechanism.append(windowManager, ts, StreamWindowManager.LANDMARK_SENTINEL);
            LandmarkWindow window = windowManager.getLandmarkWindow(activeLWID);
            window.append(ts, value);
            windowManager.putLandmarkWindow(activeLWID, window);
        }
    }

    void startLandmark(long ts) throws StreamException, BackingStoreException {
        if (ts <= tLastAppend || ts < tLastLandmarkStart || ts <= tLastLandmarkEnd) {
            throw new StreamException("attempting to retroactively start landmark");
        }
        if (activeLWID != -1) {
            return;
        }
        tLastLandmarkStart = ts;
        activeLWID = nextLWID++;
        windowManager.putLandmarkWindow(activeLWID, new LandmarkWindow(activeLWID, ts));
    }

    void endLandmark(long ts) throws StreamException, BackingStoreException {
        if (ts < tLastAppend || ts < tLastLandmarkStart || ts <= tLastLandmarkEnd) {
            throw new StreamException("attempting to retroactively end landmark");
        }
        tLastLandmarkEnd = ts;
        LandmarkWindow window = windowManager.getLandmarkWindow(activeLWID);
        window.close(ts);
        windowManager.putLandmarkWindow(activeLWID, window);
        activeLWID = -1;
    }

    Object query(int operatorNum, long t0, long t1, Object[] queryParams) throws BackingStoreException {
        long T0 = stats.getTimeRangeStart(), T1 = stats.getTimeRangeEnd();
        if (t0 > T1) {
            return operators[operatorNum].getEmptyQueryResult();
        } else if (t0 < T0) {
            t0 = T0;
        }
        if (t1 > T1) {
            t1 = T1;
        }

        Stream<SummaryWindow> summaryWindows = windowManager.getSummaryWindowsOverlapping(t0, t1);
        Stream<LandmarkWindow> landmarkWindows = windowManager.getLandmarkWindowsOverlapping(t0, t1);
        Function<SummaryWindow, Object> summaryRetriever = b -> b.aggregates[operatorNum];
        try {
            return operators[operatorNum].query(
                    stats, summaryWindows, summaryRetriever, landmarkWindows, t0, t1, queryParams);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof BackingStoreException) {
                throw (BackingStoreException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    long getNumSummaryWindows() {
        return windowManager.getNumSummaryWindows();
    }

    long getNumLandmarkWindows() {
        return windowManager.getNumLandmarkWindows();
    }

    void printWindows(boolean printPerWindowState) throws BackingStoreException {
        windowManager.printWindows(printPerWindowState, stats);
    }
}
