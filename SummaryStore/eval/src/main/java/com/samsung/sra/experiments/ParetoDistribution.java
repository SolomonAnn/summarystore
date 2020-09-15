/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;

import java.util.SplittableRandom;

public class ParetoDistribution implements Distribution<Long> {
    private final double xm, alpha;

    public ParetoDistribution(Toml conf) {
        this.xm = conf.getDouble("xm");
        this.alpha = conf.getDouble("alpha");
    }

    public ParetoDistribution(double xm, double alpha) {
        this.xm = xm;
        this.alpha = alpha;
    }

    @Override
    public Long next(SplittableRandom random) {
        return (long)Math.ceil(xm / Math.pow(random.nextDouble(), 1 / alpha));
    }
}
