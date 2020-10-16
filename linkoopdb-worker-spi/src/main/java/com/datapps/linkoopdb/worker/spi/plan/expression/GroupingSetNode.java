package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

/**
 * Created by gloway on 2019/2/15.
 */
public class GroupingSetNode extends RexNode {

    public List<RexNode> groupByExprs;
    public boolean rollup;

    public boolean foldable = false;

    public GroupingSetNode(List<RexNode> groupByExprs, boolean rollup) {
        this.groupByExprs = groupByExprs;
        this.rollup = rollup;
    }

    @Override
    public List<RexNode> getChildren() {
        return groupByExprs;
    }

    @Override
    public String simpleString() {
        String rollupOrCube;
        if (rollup) {
            rollupOrCube = "rollup";
        } else {
            rollupOrCube = "cube";
        }
        return rollupOrCube + mkString(getChildren());
    }

    @Override
    public GroupingSetNode withNewChildren(List<RexNode> children) {
        if (!groupByExprs.equals(children)) {
            this.groupByExprs = children;
            return new GroupingSetNode(this.groupByExprs, this.rollup);
        }
        return this;
    }

}
