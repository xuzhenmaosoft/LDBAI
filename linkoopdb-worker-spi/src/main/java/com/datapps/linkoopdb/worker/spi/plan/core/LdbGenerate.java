package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexCall;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

public class LdbGenerate extends SingleRel {

    public RexCall generatorRexNode;
    public List<Integer> unrequiredChildIndex;
    public boolean outer;
    public String qualifier;
    public List<RexNode> generatorOutput;


    public LdbGenerate(RelNode child) {
        super(child);
    }

    public LdbGenerate(RexCall generatorRexNode, List<Integer> unrequiredChildIndex, boolean outer, String qualifier, List<RexNode> generatorOutput,
        RelNode child) {
        super(child);
        this.generatorRexNode = generatorRexNode;
        this.unrequiredChildIndex = unrequiredChildIndex;
        this.outer = outer;
        this.qualifier = qualifier;
        this.generatorOutput = generatorOutput;
    }

    @Override
    public List<RexNode> buildOutput() {
        return ImmutableList.of(generatorRexNode);
    }

    @Override
    public LdbGenerate transformExpression(Transformer transformer) {
        List<RexNode> afterRule = transformExpression(generatorOutput, transformer);
        if (!this.generatorOutput.equals(afterRule)) {
            this.generatorOutput = afterRule;
            return new LdbGenerate(this.generatorRexNode, this.unrequiredChildIndex, this.outer, this.qualifier, this.generatorOutput, this.child);
        }
        return this;
    }

    @Override
    public LdbGenerate withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            this.child = children.get(0);
            return new LdbGenerate(this.generatorRexNode, this.unrequiredChildIndex, this.outer, this.qualifier, this.generatorOutput, this.child);
        }
        return this;
    }

    @Override
    protected String argString() {
        return generatorRexNode.simpleString();
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of();
    }

    //TODO 能generate执行索引的列
    public List<NamedRexNode> requiredChildOutput() {
        return ImmutableList.of();
    }

    public List<RexNode> qualifiedGeneratorOutput() {
        generatorOutput.forEach(attribute -> {
            attribute.nullable = true;
        });
        return generatorOutput;
    }

}
