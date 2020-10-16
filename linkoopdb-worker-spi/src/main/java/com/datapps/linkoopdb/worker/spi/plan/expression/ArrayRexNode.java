package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.ArrayType;
import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by gloway on 2019/3/15.
 */
public class ArrayRexNode extends RexNode {

    List<RexNode> children;

    public ArrayRexNode(
        List<RexNode> children) {
        this.children = children;

        if (children.size() == 0) {
            dataType = new ArrayType(Type.SQL_VARCHAR, 0);
        } else {
            dataType = new ArrayType(children.get(0).dataType, ArrayType.defaultArrayCardinality);
        }
    }

    @Override
    public List<RexNode> getChildren() {
        return children;
    }

    @Override
    public RexNode withNewChildren(List<RexNode> children) {
        if (!this.children.equals(children)) {
            this.children = children;
            return new ArrayRexNode(children);
        }
        return this;
    }
}
