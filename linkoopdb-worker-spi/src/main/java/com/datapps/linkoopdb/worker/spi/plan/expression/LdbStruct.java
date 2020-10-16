package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by gloway on 2019/4/1.
 */
public class LdbStruct extends RexNode {

    public List<RexNode> children;

    public LdbStruct(List<RexNode> children, Type structureType) {
        this.children = children;
        this.dataType = structureType;
    }

    @Override
    public List<RexNode> getChildren() {
        return children;
    }

    @Override
    public RexNode withNewChildren(List<RexNode> children) {
        return new LdbStruct(children, this.dataType);
    }
}
