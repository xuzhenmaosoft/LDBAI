package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.ExprId;

/**
 * Created by gloway on 2019/1/15.
 */
public class AliasRexNode extends NamedRexNode {

    public RexNode child;

    public AliasRexNode(RexNode child, String name) {
        this(child, name, NamedRexNode.newExprId());
    }

    public AliasRexNode(RexNode child, String name, ExprId exprId) {
        this.child = child;
        this.dataType = child.dataType;
        this.name = name;
        this.exprId = exprId;
        qualifier = ImmutableList.of();
    }

    public AliasRexNode(RexNode child, NamedRexNode name) {
        this(child, name.name, name.exprId);
        this.dataType = name.dataType;
        qualifier = ImmutableList.of();
    }

    public AliasRexNode(RexNode child, NamedRexNode name, ExprId exprId) {
        this.child = child;
        this.name = name.name;
        this.exprId = exprId;
        this.dataType = name.dataType;
        qualifier = ImmutableList.of();
    }

    @Override
    public List<RexNode> getChildren() {
        return ImmutableList.of(child);
    }

    @Override
    public String simpleString() {
        return child.simpleString() + " AS " + super.simpleString();
    }

    @Override
    public AliasRexNode withNewChildren(List<RexNode> children) {
        if (!child.fastEquals(children.get(0))) {
            this.child = children.get(0);
            return new AliasRexNode(this.child, this.name, this.exprId);
        }
        return this;
    }

    @Override
    public AttributeRexNode toAttributeRef() {
        return new AttributeRexNode(this.name, child.dataType, child.nullable, exprId, qualifier);
    }
}
