package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/8.
 */
public class TableScan extends RelNode {

    public String table;

    @Override
    public List<RexNode> buildOutput() {
        return null;
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of();
    }

    @Override
    public TableScan transformExpression(Transformer transformer) {
        return this;
    }

    @Override
    public TableScan withNewChildren(List<RelNode> children) {
        return this;
    }
}
