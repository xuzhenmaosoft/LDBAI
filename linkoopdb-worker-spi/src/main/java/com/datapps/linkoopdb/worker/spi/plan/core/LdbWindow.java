package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SortOrderRexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/8.
 */
public class LdbWindow extends SingleRel {

    public List<NamedRexNode> windowExpressions;
    public List<RexNode> partitionSpec;
    public List<SortOrderRexNode> orderSpec;

    public LdbWindow(List<NamedRexNode> windowExpressions, List<RexNode> partitionSpec, List<SortOrderRexNode> orderSpec, RelNode child) {
        super(child);
        this.windowExpressions = windowExpressions;
        this.partitionSpec = partitionSpec;
        this.orderSpec = orderSpec;
    }

    @Override
    public List<RexNode> buildOutput() {
        List<RexNode> output = new ArrayList<>();
        output.addAll(child.getOutput());
        output.addAll(windowExpressions.stream().map(NamedRexNode::toAttributeRef).collect(
            Collectors.toList()));
        return ImmutableList.copyOf(output);
    }

    @Override
    public List<RexNode> expressions() {
        List<RexNode> output = new ArrayList<>();
        output.addAll(child.getOutput());
        output.addAll(windowExpressions);
        return ImmutableList.copyOf(output);
    }

    @Override
    public LdbWindow transformExpression(Transformer transformer) {
        List<NamedRexNode> windowExpressionsAfterRule = transformExpression((List) windowExpressions, transformer);
        List<RexNode> partitionSpecAfterRule = transformExpression(partitionSpec, transformer);
        List<SortOrderRexNode> orderSpecAfterRule = transformExpression((List) orderSpec, transformer);

        if (!windowExpressions.equals(windowExpressionsAfterRule)
            || !partitionSpec.equals(partitionSpecAfterRule)
            || !orderSpec.equals(orderSpecAfterRule)) {
            this.windowExpressions = windowExpressionsAfterRule;
            this.partitionSpec = partitionSpecAfterRule;
            this.orderSpec = orderSpecAfterRule;
            return new LdbWindow(windowExpressions, partitionSpec, orderSpec, child);
        }
        return this;
    }

    @Override
    public LdbWindow withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            this.child = children.get(0);
            return new LdbWindow(this.windowExpressions, this.partitionSpec, this.orderSpec,
                this.child);
        }
        return this;
    }
}
