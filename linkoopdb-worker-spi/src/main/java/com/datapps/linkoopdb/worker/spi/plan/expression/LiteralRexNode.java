package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by gloway on 2019/1/17.
 */
public class LiteralRexNode extends RexNode {

    public static final LiteralRexNode DEFAULT_KEYWORD = new LiteralRexNode("__default__", Type.SQL_VARCHAR);

    public Object value;

    public LiteralRexNode(Object value, Type dataType) {
        this.value = value;
        this.dataType = dataType;
        this.foldable = true;
        this.nullable = value == null;
    }

    @Override
    public List<RexNode> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public LiteralRexNode withNewChildren(List<RexNode> children) {
        return this;
    }

    @Override
    public String simpleString() {
        return value == null ? "null" : value.toString();
    }
}
