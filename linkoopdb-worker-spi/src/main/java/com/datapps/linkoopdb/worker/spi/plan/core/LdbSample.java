package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/8.
 */
public class LdbSample extends SingleRel {

    public double lowerBound;
    public double upperBound;
    public boolean withReplacement;
    public long seed;

    public LdbSample(RelNode child, double lowerBound, double upperBound, boolean withReplacement, long seed) {
        super(child);
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.withReplacement = withReplacement;
        this.seed = seed;
    }

    @Override
    public LdbSample withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            this.child = children.get(0);
            return new LdbSample(this.child, this.lowerBound, this.upperBound, this.withReplacement,
                this.seed);
        }
        return this;
    }

    @Override
    protected String argString() {
        return lowerBound + ", " + upperBound + ", " + withReplacement + ", " + seed;
    }

    @Override
    public List<RexNode> buildOutput() {
        return child.getOutput();
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of();
    }

    @Override
    public LdbSample transformExpression(Transformer transformer) {
        return this;
    }
}
