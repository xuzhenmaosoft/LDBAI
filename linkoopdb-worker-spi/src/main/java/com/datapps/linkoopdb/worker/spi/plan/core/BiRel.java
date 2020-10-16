package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;

/**
 * Created by gloway on 2019/1/8.
 */
public abstract class BiRel extends RelNode {

    protected RelNode left;
    protected RelNode right;

    @Override
    public final List<RelNode> getChildren() {
        return ImmutableList.of(left, right);
    }

    public RelNode getLeft() {
        return left;
    }

    public RelNode getRight() {
        return right;
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of();
    }

}
