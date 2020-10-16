package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.AttributeRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexCall;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SubqueryRexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * @author xingbu
 * @version 1.0 <p> created by　19-1-24 下午5:59
 *
 * LdbTableFunction
 */
public class LdbTableFunction extends RelNode {

    protected RexNode functionRexNode;
    public List<AttributeRexNode> columnList;

    public LdbTableFunction(
        RexNode functionCall,
        List<AttributeRexNode> columnList) {
        this.functionRexNode = functionCall;
        this.columnList = columnList;
    }

    @Override
    public List<RexNode> buildOutput() {
        return ImmutableList.copyOf(columnList);
    }

    @Override
    public LdbTableFunction transformExpression(Transformer transformer) {
        return this;
    }

    @Override
    public List<RelNode> getInnerChildren() {
        ArrayList<RelNode> subQuery = new ArrayList<>();
        if (functionRexNode instanceof RexCall) {
            for (RexNode rexNode : ((RexCall) functionRexNode).operands) {
                rexNode.foreach(rel -> {
                    if (rel instanceof SubqueryRexNode) {
                        subQuery.add(((SubqueryRexNode) rel).plan);
                    }
                });
            }
        }
        return subQuery;
    }

    @Override
    public List<RexNode> expressions() {
        return getOutput();
    }


    public LdbTableFunction withNewChildren(List<RelNode> childrens) {
        return new LdbTableFunction(this.functionRexNode, this.columnList);
    }

    public RexNode getFunctionRexNode() {
        return functionRexNode;
    }

    public void setFunctionRexNode(RexNode functionRexNode) {
        this.functionRexNode = functionRexNode;
    }
}
