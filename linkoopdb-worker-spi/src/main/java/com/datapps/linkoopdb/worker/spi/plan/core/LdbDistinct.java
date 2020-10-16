package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/16.
 */
public class LdbDistinct extends SingleRel {

    public LdbDistinct(RelNode child) {
        super(child);
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
    public LdbDistinct transformExpression(Transformer transformer) {
        return this;
    }

    @Override
    public LdbDistinct withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            this.child = children.get(0);
            return new LdbDistinct(this.child);
        }
        return this;
    }
}
