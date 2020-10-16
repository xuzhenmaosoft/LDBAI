package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * @author xingbu
 * @version 1.0
 *
 * created by　19-3-22 下午12:26
 */
public abstract class LdbFunctionRel extends RelNode {

    public enum FunctionRelType {
        SPARK_MODEL_TRANSFORM,
        EXTERNAL_MODEL_TRANSFORM,
        SPARK_MODEL_TRAIN,
        EXTERNAL_MODEL_LOAD,
        TENSORFLOW_MODEL_TRAIN,
        TENSORFLOW_MODEL_TRANSFORM,
        PIPELINE_BUILD,
        PIPELINE_APPLY,
        PIPELINE_STAGE,
        GRAPH_SIMPLE_BUILD,
        GRAPH_BUILD,
        GRAPH_COMPUTE,
    }

    public boolean isBuilt;
    protected FunctionRelType type;
    protected LdbSource source;

    public LdbFunctionRel(FunctionRelType type, LdbSource source) {
        this.type = type;
        this.source = source;
    }

    public FunctionRelType getType() {
        return type;
    }

    public void setType(FunctionRelType type) {
        this.type = type;
    }

    public LdbSource getSource() {
        return source;
    }

    public void setSource(LdbSource source) {
        this.source = source;
    }

    @Override
    public List<RexNode> buildOutput() {
        return ImmutableList.of();
    }

    @Override
    public RelNode transformExpression(Transformer transformer) {
        return this;
    }

    @Override
    public RelNode withNewChildren(List<RelNode> children) {
        return this;
    }


    public String logMessage() {
        return argString();
    }

}
