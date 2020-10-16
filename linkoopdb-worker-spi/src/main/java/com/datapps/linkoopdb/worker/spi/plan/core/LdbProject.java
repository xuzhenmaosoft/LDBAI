package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.AliasRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/8.
 */
public class LdbProject extends SingleRel {

    public List<RexNode> exps;

    public LdbProject(List<RexNode> projects, RelNode child) {
        super(child);
        this.exps = projects;
    }

    @Override
    public List<RexNode> buildOutput() {
        List convertList = new ArrayList();
        exps.forEach(rexNode -> {
            if (rexNode instanceof AliasRexNode) {
                convertList.add(((AliasRexNode) rexNode).toAttributeRef());
            } else {
                convertList.add(rexNode);
            }
        });
        return ImmutableList.copyOf(convertList);
    }

    @Override
    protected String argString() {
        return mkString(exps);
    }

    @Override
    public List<RexNode> expressions() {
        return exps;
    }

    @Override
    public LdbProject transformExpression(Transformer transformer) {
        List<RexNode> expsAfterRule = transformExpression(exps, transformer);
        if (!this.exps.equals(expsAfterRule)) {
            this.exps = expsAfterRule;
            return new LdbProject(this.exps, this.child);
        }
        return this;
    }

    public LdbProject transformExpressionUp(Transformer transformer) {
        List<RexNode> expsAfterRule = transformExpressionUp(exps, transformer);
        if (!this.exps.equals(expsAfterRule)) {
            this.exps = expsAfterRule;
            return new LdbProject(this.exps, this.child);
        }
        return this;
    }

    @Override
    public LdbProject withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            return new LdbProject(this.exps, children.get(0));
        }
        return this;
    }
}
