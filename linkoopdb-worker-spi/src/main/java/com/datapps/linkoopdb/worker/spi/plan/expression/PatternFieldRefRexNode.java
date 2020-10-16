package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;

public class PatternFieldRefRexNode extends NamedRexNode {

    private final String alpha;
    private final NamedRexNode fieldRexNode;

    public PatternFieldRefRexNode(String alpha, NamedRexNode fieldRexNode) {
        this.alpha = alpha;
        this.fieldRexNode = Objects.requireNonNull(fieldRexNode);

        this.name = fieldRexNode.name;
        this.dataType = fieldRexNode.dataType;
        this.nullable = fieldRexNode.nullable;
        this.exprId = fieldRexNode.exprId;
        this.qualifier = fieldRexNode.qualifier;
    }

    @Override
    public List<RexNode> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public RexNode withNewChildren(List<RexNode> children) {
        return this;
    }

    @Override
    public AttributeRexSet references() {
        return new AttributeRexSet(ImmutableList.of(this));
    }

    @Override
    public AttributeRexNode toAttributeRef() {
        return fieldRexNode.toAttributeRef();
    }

    @Override
    public String simpleString() {
        if (alpha != null) {
            return alpha + "." + fieldRexNode.simpleString();
        }
        return super.simpleString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PatternFieldRefRexNode)) {
            return false;
        }

        PatternFieldRefRexNode objPattern = (PatternFieldRefRexNode) obj;
        return Objects.equals(objPattern.alpha, this.alpha)
            && Objects.equals(objPattern.toAttributeRef(), this.toAttributeRef());
    }

    public String getAlpha() {
        return alpha;
    }

    public NamedRexNode getFieldRexNode() {
        return fieldRexNode;
    }
}
