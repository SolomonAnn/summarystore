package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStore.*;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Configuration backed by a Toml file. See example.toml for a sample config.
 *
 * Raises IllegalArgumentException on some parse errors but is generally optimistic and expects the file is a legal
 * config.
 */
public class Configuration {
    private final Toml toml;

    public Configuration(File file) {
        if (!file.isFile()) throw new IllegalArgumentException("invalid or non-existent config file " + file);
        toml = new Toml().read(file);
    }

    /** Get working directory, where all files will be input/output */
    public String getDirectory() {
        return toml.getString("directory");
    }

    /** Optional prefix to add onto every input file */
    public String getPrefix() {
        return toml.getString("prefix", "");
    }

    /** Size of SummaryStore bucket cache */
    public long getBucketCacheSize() {
        return toml.getLong("bucket-cache-size", 0L);
    }

    public int getIngestBufferSize() {
        return toml.getLong("ingest-buffer-size", 0L).intValue();
    }

    /** Get prefix of all SummaryStore output files. {@link #getHash} explains why we use a hash here */
    public String getStorePrefix(String decayName) {
        return String.format("%s/%sS%s.D%s", getDirectory(), getPrefix(), getHash(toml.getTable("data")), decayName);
    }

    public String getWorkloadFile() {
        return String.format("%s/%sS%s.W%s.workload",
                getDirectory(), getPrefix(), getHash(toml.getTable("data")), getHash(toml.getTable("workload")));
    }

    public String getProfileFile() {
        return String.format("%s/%sS%s.W%s.D%s.profile", getDirectory(), getPrefix(),
                getHash(toml.getTable("data")), getHash(toml.getTable("workload")), getHash(toml.getList("decay-functions")));
    }

    /** Data/queries will span the time range [tstart, tend] */
    public long getTstart() {
        return toml.getLong("data.tstart");
    }

    /** Data/queries will span the time range [tstart, tend] */
    public long getTend() {
        return toml.getLong("data.tend");
    }

    public StreamGenerator getStreamGenerator() {
        Toml conf = toml.getTable("data");
        return constructObjectViaReflection(
                "com.samsung.sra.DataStoreExperiments." + conf.getString("stream-generator"),
                conf);
    }

    /**
     * Compute a deterministic hash of some portion of the toml tree.
     * Use case: suppose we run several experiments trying various workloads against the same dataset. We want to
     * recognize that we can use the same generated SummaryStores for all these experiments. To do this we will name
     * SummaryStores using a hash of the dataset config portion of the toml.
     */
    public static String getHash(Object node) {
        HashCodeBuilder builder = new HashCodeBuilder();
        buildHash(node, builder);
        return Long.toString((long)builder.toHashCode() - (long)Integer.MIN_VALUE);
    }

    private static void buildHash(Object node, HashCodeBuilder builder) {
        if (node instanceof List) {
            for (Object entry: (List)node) { // Array. Hash entries in sequence
                buildHash(entry, builder);
            }
        } else if (node instanceof Toml || node instanceof Map) { // Table. Hash entries in key-sorted order
            Map<String, Object> map = node instanceof Toml ?
                    ((Toml) node).toMap() :
                    (Map<String, Object>)node;
            map.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> {
                        builder.append(e.getKey());
                        buildHash(e.getValue(), builder);
                    });
        } else { // a primitive
            assert node instanceof Number || node instanceof String || node instanceof Character
                    : "unknown node class " + node.getClass();
            builder.append(node); // TODO: verify that HashCodeBuilder handles Object-casted primitives sensibly
        }
    }

    private static <T> T constructObjectViaReflection(String className, Toml conf) {
        try {
            return (T) Class.forName(className).getConstructor(Toml.class).newInstance(conf);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("could not construct object of type " + className, e);
        }
    }

    /** Return list of all decay function names. Use parseDecayFunction to convert name to Windowing */
    public List<String> getDecayFunctions() {
        return toml.getList("decay-functions");
    }

    /** Convert decay function name to Windowing */
    public Windowing parseDecayFunction(String decayName) {
        if (decayName == null) {
            throw new IllegalArgumentException("expect non-null decay spec");
        } else if (decayName.startsWith("exponential")) {
            return new GenericWindowing(new ExponentialWindowLengths(Double.parseDouble(decayName.substring("exponential".length()))));
        } else if (decayName.startsWith("rationalPower")) {
            String[] pq = decayName.substring("rationalPower".length()).split(",");
            if (pq.length != 2 && pq.length != 4) throw new IllegalArgumentException("malformed rationalPower decay spec " + decayName);
            int P, Q, R, S;
            P = Integer.parseInt(pq[0]);
            Q = Integer.parseInt(pq[1]);
            if (pq.length == 4) {
                R = Integer.parseInt(pq[2]);
                S = Integer.parseInt(pq[3]);
            } else {
                R = 1;
                S = 1;
            }
            return new RationalPowerWindowing(P, Q, R, S);
        } else {
            throw new IllegalArgumentException("unrecognized decay function " + decayName);
        }
    }

    public WindowOperator[] getOperators() {
        List<String> operatorNames = toml.getList("data.operators");
        WindowOperator[] operators = new WindowOperator[operatorNames.size()];
        for (int i = 0; i < operators.length; ++i) {
            String opname = operatorNames.get(i);
            try {
                operators[i] = (WindowOperator)Class.forName("com.samsung.sra.DataStore.Aggregates." + opname).newInstance();
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("could not construct operator " + opname, e);
            }
        }
        return operators;
    }

    public WorkloadGenerator getWorkloadGenerator() {
        Toml conf = toml.getTable("workload");
        return constructObjectViaReflection(
                "com.samsung.sra.DataStoreExperiments." + conf.getString("workload-generator"),
                conf);
    }

    public static Distribution<Long> parseDistribution(Toml conf) {
        return constructObjectViaReflection(
                "com.samsung.sra.DataStoreExperiments." + conf.getString("distribution"),
                conf);
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration(
                new File("/Users/a.vulimiri/samsung/summarystore/code/SummaryStore/example.toml"));
        System.out.println(getHash(config.toml.getTable("data")));
    }
}