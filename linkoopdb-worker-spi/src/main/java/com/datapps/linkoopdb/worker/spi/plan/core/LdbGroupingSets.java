package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/3/11.
 */
public class LdbGroupingSets extends SingleRel {

    public List<List<RexNode>> selectedGroupByExprs;
    public List<RexNode> groupByExprs;
    public List<NamedRexNode> aggregations;

    public LdbGroupingSets(List<List<RexNode>> selectedGroupByExprs,
        List<RexNode> groupByExprs,
        RelNode child,
        List<NamedRexNode> aggregations) {
        super(child);
        this.selectedGroupByExprs = selectedGroupByExprs;
        this.groupByExprs = groupByExprs;
        this.aggregations = aggregations;
    }

    @Override
    public List<RexNode> buildOutput() {
        return ImmutableList.copyOf(aggregations);
    }

    @Override
    public List<RexNode> expressions() {
        return getOutput();
    }

    @Override
    public LdbGroupingSets transformExpression(Transformer transformer) {
        List selectedGroupAfterRule = selectedGroupByExprs.stream().map(list ->
            transformExpression(list, transformer)).collect(Collectors.toList());
        List<RexNode> groupByExprsAfterRule = transformExpression(groupByExprs, transformer);
        List<NamedRexNode> aggregationsAfterRule = transformExpression((List) aggregations, transformer);

        if (!selectedGroupAfterRule.equals(selectedGroupByExprs)
            || !groupByExprsAfterRule.equals(groupByExprs)
            || !aggregationsAfterRule.equals(aggregations)) {
            this.selectedGroupByExprs = selectedGroupAfterRule;
            this.groupByExprs = groupByExprsAfterRule;
            this.aggregations = aggregationsAfterRule;
            return new LdbGroupingSets(this.selectedGroupByExprs, this.groupByExprs, this.child, this.aggregations);
        }
        return this;
    }

    @Override
    public LdbGroupingSets withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            this.child = children.get(0);
            return new LdbGroupingSets(this.selectedGroupByExprs, this.groupByExprs, this.child,
                this.aggregations);
        }
        return this;
    }
}
