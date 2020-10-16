package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.ExprId;

/**
 * Created by gloway on 2019/1/9.
 */
public class AttributeRexNode extends NamedRexNode {

    public AttributeRexNode(String name, Type dataType) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = true;
        this.exprId = NamedRexNode.newExprId();
        this.qualifier = ImmutableList.of();
    }

    public AttributeRexNode(String name, Type dataType, boolean nullable, ExprId exprId, List<String> qualifier) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.exprId = exprId;
        this.qualifier = qualifier;
    }

    @Override
    public List<RexNode> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public AttributeRexNode withNewChildren(List<RexNode> children) {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AttributeRexNode) {
            AttributeRexNode ar = (AttributeRexNode) obj;
            return Objects.equals(name, ar.name) && dataType.equals(ar.dataType) && nullable == ar.nullable
                && exprId.equals(ar.exprId);
        } else {
            return false;
        }
    }

    @Override
    public AttributeRexSet references() {
        return new AttributeRexSet(ImmutableList.of(this));
    }

    @Override
    public AttributeRexNode toAttributeRef() {
        return this;
    }

}
