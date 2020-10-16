package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.jdbc.types.ArrayType;

/**
 * Created by gloway on 2019/4/1.
 */
public class LdbGetArrayItem extends RexNode {

    public RexNode child;
    public RexNode ordinal;

    public LdbGetArrayItem(RexNode child,
        RexNode ordinal) {
        this.child = child;
        this.ordinal = ordinal;
        ArrayType dataType = (ArrayType) child.dataType;
        this.dataType = dataType.getDataType();
    }

    @Override
    public List<RexNode> getChildren() {
        return ImmutableList.of(child, ordinal);
    }

    @Override
    public RexNode withNewChildren(List<RexNode> children) {
        if (!child.equals(children.get(0)) || !ordinal.equals(children.get(1))) {
            this.child = children.get(0);
            this.ordinal = children.get(1);
            return new LdbGetArrayItem(children.get(0), children.get(1));
        }
        return this;
    }
}
