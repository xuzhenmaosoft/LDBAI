package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.AliasRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/8.
 */
public class LdbAggregate extends SingleRel {

    public List<RexNode> groupingExpressions;
    public List<NamedRexNode> aggregateExpressions;

    public LdbAggregate(
        List<RexNode> groupingExpressions,
        List<NamedRexNode> aggregateExpressions,
        RelNode child) {
        super(child);
        this.groupingExpressions = ImmutableList.copyOf(groupingExpressions);
        this.aggregateExpressions = ImmutableList.copyOf(aggregateExpressions);
    }

    @Override
    public List<RexNode> buildOutput() {
        List convertList = new ArrayList();
        aggregateExpressions.forEach(rexNode -> {
            if (rexNode instanceof AliasRexNode) {
                convertList.add(((AliasRexNode) rexNode).toAttributeRef());
            } else {
                convertList.add(rexNode);
            }
        });
        return ImmutableList.copyOf(convertList);
    }

    @Override
    public List<RexNode> expressions() {
        return getOutput();
    }

    @Override
    public LdbAggregate withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            return new LdbAggregate(this.groupingExpressions, this.aggregateExpressions,
                children.get(0));
        }
        return this;
    }

    @Override
    protected String argString() {
        return mkString(groupingExpressions) + ", " + super.argString();
    }

    @Override
    public LdbAggregate transformExpression(Transformer transformer) {
        List<RexNode> groupingAfterRule = transformExpression(groupingExpressions, transformer);
        List<NamedRexNode> aggregateAfterRule = transformExpression((List) aggregateExpressions, transformer);
        if (!groupingExpressions.equals(groupingAfterRule)
            || !aggregateExpressions.equals(aggregateAfterRule)) {
            groupingExpressions = groupingAfterRule;
            aggregateExpressions = aggregateAfterRule;
            return new LdbAggregate(groupingExpressions, aggregateExpressions, child);
        }
        return this;
    }

}
