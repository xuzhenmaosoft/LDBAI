package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * @author xingbu
 * @version 1.0
 *
 * 继承该接口的可以作为pipeline的stage created by　19-6-24 下午2:46
 */
public class LdbPipelineStage extends LdbFunctionRel implements LdbPipelineStageEntity {

    // 每个Stage所依赖的实体 目前有LdbModel和LdbPipeline
    private RelNode stageEntity;
    private List<RexNode> inputCols;
    private List<RexNode> outputCols;
    protected RexNode stageFunction;
    protected String stepName;

    public LdbPipelineStage(FunctionRelType type, String stepName) {
        super(type, null);
        this.stepName = stepName;
    }

    public LdbPipelineStage(FunctionRelType type, LdbSource source) {
        super(type, source);
    }

    @Override
    protected String argString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        stringBuilder.append("TYPE(");
        switch (type) {
            case PIPELINE_STAGE:
                stringBuilder.append("PIPELINE_STAGE");
                break;
        }
        stringBuilder.append(") STAGE_TYPE(");
        stringBuilder.append(stageEntity.getClass().getSimpleName());
        stringBuilder.append(")]");
        return stringBuilder.toString();
    }

    @Override
    public List<RexNode> buildOutput() {
        return ImmutableList.copyOf(getOutputCols());
    }

    @Override
    public RelNode transformExpression(Transformer transformer) {
        return this;
    }

    @Override
    public List<RexNode> expressions() {
        return getOutputCols();
    }

    @Override
    public RelNode withNewChildren(List<RelNode> children) {
        return this;
    }


    @Override
    public List<RexNode> getInputCols() {
        return inputCols;
    }

    @Override
    public List<RexNode> getOutputCols() {
        return outputCols;
    }

    @Override
    public void setInputCols(List<RexNode> inputCols) {
        if (stageEntity != null) {
            ((LdbPipelineStageEntity) stageEntity).setInputCols(inputCols);
        }
        this.inputCols = inputCols;
    }

    @Override
    public void setOutputCols(List<RexNode> outputCols) {
        if (stageEntity != null) {
            ((LdbPipelineStageEntity) stageEntity).setOutputCols(outputCols);
        }
        this.outputCols = outputCols;
    }

    @Override
    public LdbPipelineStageEntity getStageEntity() {
        return ((LdbPipelineStageEntity) stageEntity);
    }


    @Override
    public RexNode getFunctionRexNode() {
        return ((LdbPipelineStageEntity) stageEntity).getFunctionRexNode();
    }

    @Override
    public void setFunctionRexNode(RexNode function) {
        ((LdbPipelineStageEntity) stageEntity).setFunctionRexNode(function);
    }

    public void setStageEntity(LdbPipelineStageEntity stageEntity) {
        this.stageEntity = (RelNode) stageEntity;
        this.inputCols = stageEntity.getInputCols();
        this.outputCols = stageEntity.getOutputCols();
    }

    public String getStepName() {
        return stepName;
    }
}
