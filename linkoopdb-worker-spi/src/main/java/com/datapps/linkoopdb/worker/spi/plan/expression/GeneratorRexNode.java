package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class GeneratorRexNode extends RexNode {


    public List<RexNode> operands;

    public GeneratorRexNode(List<RexNode> operands) {
        this.operands = operands;
    }

    @Override
    public List<RexNode> getChildren() {
        return ImmutableList.copyOf(operands);
    }

    @Override
    public GeneratorRexNode withNewChildren(List<RexNode> children) {
        if (!operands.equals(children)) {
            this.operands = children;
            return new GeneratorRexNode(this.operands);
        }
        return this;
    }

    @Override
    public String simpleString() {
        return mkString(operands);
    }


}
