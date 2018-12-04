package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;

import java.util.SplittableRandom;

public class ParetoDistribution implements Distribution<Long> {
    private final double xm, alpha;

    public ParetoDistribution(Toml conf) {
        this.xm = conf.getDouble("xm");
        this.alpha = conf.getDouble("alpha");
    }

    @Override
    public Long next(SplittableRandom random) {
        return (long)Math.ceil(xm / Math.pow(random.nextDouble(), 1 / alpha));
    }
}