package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SortOrderRexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/8.
 */
public class LdbSort extends SingleRel {

    public List<SortOrderRexNode> order;

    public LdbSort(List<SortOrderRexNode> order, RelNode child) {
        super(child);
        this.order = ImmutableList.copyOf(order);
    }

    @Override
    public List<RexNode> buildOutput() {
        return child.getOutput();
    }

    @Override
    public LdbSort withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            this.child = children.get(0);
            return new LdbSort(this.order, this.child);
        }
        return this;
    }

    @Override
    protected String argString() {
        return mkString(order);
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of();
    }

    @Override
    public LdbSort transformExpression(Transformer transformer) {
        List<SortOrderRexNode> orderAfterRule = transformExpression((List) order, transformer);
        if (order.equals(orderAfterRule)) {
            this.order = orderAfterRule;
            return new LdbSort(this.order, this.child);
        }
        return this;
    }
}
