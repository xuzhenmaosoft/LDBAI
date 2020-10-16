package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Created by gloway on 2019/1/9.
 */
public abstract class SingleRexNode extends RexNode {


    public RexNode child;

    public boolean foldable;
    public boolean nullable;

    public SingleRexNode(RexNode child) {
        this.child = child;
        this.foldable = child.foldable;
        this.nullable = child.nullable;
    }

    @Override
    public final List<RexNode> getChildren() {
        return ImmutableList.of(child);
    }

}
