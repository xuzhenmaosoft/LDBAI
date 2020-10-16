package com.datapps.linkoopdb.worker.spi.plan.func;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;

/**
 * Created by gloway on 2019/1/29.
 */
public class SqlWindowFrame extends RexNode {

    public WindowFrameType frameType;
    public RexWindowBound lower;
    public RexWindowBound upper;

    public boolean unresolved;

    public SqlWindowFrame() {
        unresolved = true;
    }

    public SqlWindowFrame(WindowFrameType frameType, RexWindowBound lower, RexWindowBound upper) {

        this.frameType = frameType;
        this.lower = lower;
        this.upper = upper;
    }

    @Override
    public String simpleString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("specifiedwindowframe(");
        stringBuilder.append(frameType).append(", ").append(lower).append(", ").append(upper);
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    public List<RexNode> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public SqlWindowFrame withNewChildren(List<RexNode> children) {
        return this;
    }
}
