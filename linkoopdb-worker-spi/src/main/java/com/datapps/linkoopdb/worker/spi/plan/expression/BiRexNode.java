package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Created by gloway on 2019/1/9.
 */
public abstract class BiRexNode extends RexNode {

    protected RexNode left;
    protected RexNode right;

    public boolean foldable = left.foldable && right.foldable;
    public boolean nullable = left.nullable || right.nullable;

    @Override
    public final List<RexNode> getChildren() {
        return ImmutableList.of(left, right);
    }

    public RexNode getLeft() {
        return left;
    }

    public RexNode getRight() {
        return right;
    }
}
