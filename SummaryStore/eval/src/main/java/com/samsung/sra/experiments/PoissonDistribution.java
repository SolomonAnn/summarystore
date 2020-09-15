package com.samsung.sra.experiments;

import java.util.SplittableRandom;

public class PoissonDistribution implements Distribution<Long> {
    private final int lambda;

    public PoissonDistribution(int lambda) {
        this.lambda = lambda;
    }

    @Override
    public Long next(SplittableRandom random) {
        double l = Math.exp(-lambda);
        long k = 0;
        double p = 1.0;
        do {
            k++;
            p *= random.nextDouble();;
        } while (p >= l);
        return k - 1;
    }
}
