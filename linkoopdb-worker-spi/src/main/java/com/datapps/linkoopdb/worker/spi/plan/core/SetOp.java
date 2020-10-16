package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;

/**
 * Created by gloway on 2019/1/10.
 */
public abstract class SetOp extends RelNode {

    public static final int NOUNION = 0,
        UNION = 1,
        INTERSECT = 2,
        EXCEPT = 3;
    public final int kind;
    public final boolean all;
    private List<RelNode> children;

    public SetOp(List<RelNode> children, int kind, boolean all) {
        this.children = children;
        this.kind = kind;
        this.all = all;
    }

    @Override
    public List<RelNode> getChildren() {
        return children;
    }

    public void setChildren(List<RelNode> children) {
        this.children = children;
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of();
    }

}
