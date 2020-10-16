package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by gloway on 2019/4/1.
 */
public class LdbExtractValue extends RexNode {

    public RexNode child;
    public RexNode extraction;

    public LdbExtractValue(RexNode child,
        RexNode extraction, Type dataType) {
        this.child = child;
        this.extraction = extraction;
        this.dataType = dataType;
    }

    @Override
    public List<RexNode> getChildren() {
        return ImmutableList.of(child);
    }

    @Override
    public RexNode withNewChildren(List<RexNode> children) {
        return new LdbExtractValue(children.get(0), children.get(1), this.dataType);
    }
}
