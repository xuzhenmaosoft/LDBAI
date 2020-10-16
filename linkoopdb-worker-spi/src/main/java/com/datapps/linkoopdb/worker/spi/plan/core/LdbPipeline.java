package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;

/**
 * @author xingbu
 * @version 1.0
 *
 * created by　19-3-26 下午5:06
 */
public class LdbPipeline extends LdbPipelineStage {

    private List<LdbPipelineStage> stages;

    public LdbPipeline(FunctionRelType type, LdbSource source,
        List<LdbPipelineStage> stages) {
        super(type, source);
        this.stages = stages;
        if (type == FunctionRelType.PIPELINE_BUILD) {
            this.isBuilt = false;
        } else {
            this.isBuilt = true;
        }
        LinkedHashSet<RexNode> inputSet = new LinkedHashSet<>();
        LinkedHashSet<RexNode> outputSet = new LinkedHashSet<>();
        for (LdbPipelineStage stage : stages) {
            List<RexNode> inputCols = stage.getInputCols();
            List<RexNode> outputCols = stage.getOutputCols();
            inputSet.addAll(inputCols);
            outputSet.addAll(outputCols);
        }
        this.setInputCols(ImmutableList.copyOf(inputSet));
        this.setOutputCols(ImmutableList.copyOf(outputSet));
    }

    @Override
    public List<RelNode> getInnerChildren() {
        return ImmutableList.copyOf(stages);
    }

    @Override
    protected String argString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        stringBuilder.append("TYPE(");
        switch (type) {
            case PIPELINE_BUILD:
            case PIPELINE_APPLY:
                stringBuilder.append("PIPELINE");
                break;
        }
        stringBuilder.append(") STEPS");

        stringBuilder.append(getSteps().toString() + "]");
        return stringBuilder.toString();
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of();
    }

    public List<LdbPipelineStage> getStages() {
        return stages;
    }

    public List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        for (LdbPipelineStage stage : stages) {
            steps.add(stage.stepName);
        }
        return steps;
    }

    @Override
    public List<RexNode> getInputCols() {
        List<RexNode> inputCols = new ArrayList<>();
        for (LdbPipelineStage stage : stages) {
            inputCols.addAll(stage.getInputCols());
        }
        return ImmutableList.copyOf(inputCols);
    }

    @Override
    public LdbPipelineStageEntity getStageEntity() {
        return this;
    }

    @Override
    public RexNode getFunctionRexNode() {
        return stageFunction;
    }

    @Override
    public void setFunctionRexNode(RexNode function) {
        this.stageFunction = function;
    }

    public void setStages(List<LdbPipelineStage> stages) {
        this.stages = stages;
    }

}
