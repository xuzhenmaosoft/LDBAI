package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/8.
 */
public class LdbFilter extends SingleRel {

    protected RexNode condition;

    public LdbFilter(RexNode condition, RelNode child) {
        super(child);
        this.condition = condition;
    }

    @Override
    public List<RexNode> buildOutput() {
        return child.getOutput();
    }

    @Override
    public LdbFilter withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            this.child = children.get(0);
            return new LdbFilter(this.condition, this.child);
        }
        return this;
    }

    @Override
    protected String argString() {
        return condition.simpleString();
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of(condition);
    }

    @Override
    public LdbFilter transformExpression(Transformer transformer) {
        RexNode afterRule = condition.transformDown(transformer);
        if (!this.condition.equals(afterRule)) {
            this.condition = afterRule;
            return new LdbFilter(this.condition, this.child);
        }
        return this;
    }

}
