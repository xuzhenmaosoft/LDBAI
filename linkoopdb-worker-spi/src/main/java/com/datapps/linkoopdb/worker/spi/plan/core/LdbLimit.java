package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/16.
 */
public class LdbLimit extends SingleRel {

    public RexNode limitExpr;
    public int offset;

    public LdbLimit(RexNode limitExpr, RelNode child, int offset) {
        this(limitExpr, child);
        this.offset = offset;
    }

    public LdbLimit(RexNode limitExpr, RelNode child) {
        super(child);
        this.limitExpr = limitExpr;
    }

    @Override
    public List<RexNode> buildOutput() {
        return child.getOutput();
    }

    @Override
    public LdbLimit withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            return new LdbLimit(this.limitExpr, children.get(0), this.offset);
        }
        return this;
    }

    @Override
    protected String argString() {
        return limitExpr.simpleString();
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of();
    }

    @Override
    public LdbLimit transformExpression(Transformer transformer) {
        RexNode afterRule = limitExpr.transformDown(transformer);
        if (!limitExpr.equals(afterRule)) {
            this.limitExpr = afterRule;
            new LdbLimit(this.limitExpr, this.child, this.offset);
        }
        return this;
    }
}
