package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Created by gloway on 2019/1/9.
 */
public class RexOuterRef extends NamedRexNode {

    public NamedRexNode e;

    public RexOuterRef(NamedRexNode e) {
        this.e = e;
        this.name = e.name;
        this.qualifier = e.qualifier;
        this.exprId = e.exprId;
    }

    @Override
    public List<RexNode> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public RexOuterRef withNewChildren(List<RexNode> children) {
        return this;
    }

    @Override
    public String simpleString() {
        return "outer(" + super.simpleString() + ")";
    }

    @Override
    public AttributeRexNode toAttributeRef() {
        return e.toAttributeRef();
    }
}
