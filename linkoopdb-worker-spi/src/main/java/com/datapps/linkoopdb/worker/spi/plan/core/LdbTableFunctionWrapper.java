package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.AttributeRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;

/**
 * @author xingbu
 * @version 1.0
 *
 * 这个类是作为普通函数作为PipelineStage的包装类
 *
 * created by　19-6-24 下午2:46
 */
public class LdbTableFunctionWrapper extends LdbTableFunction implements LdbPipelineStageEntity {

    private List<RexNode> inputCols;
    private List<RexNode> outputCols;

    public LdbTableFunctionWrapper(RexNode functionCall, List<RexNode> inputCols, List<RexNode> outputCols) {
        this(functionCall, null);
        this.inputCols = inputCols;
        this.outputCols = outputCols;
    }

    public LdbTableFunctionWrapper(RexNode functionCall,
        List<AttributeRexNode> columnList) {
        super(functionCall, columnList);
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
        this.inputCols = inputCols;
    }

    @Override
    public void setOutputCols(List<RexNode> outputCols) {
        this.outputCols = outputCols;
    }

    @Override
    public LdbPipelineStageEntity getStageEntity() {
        return this;
    }

    @Override
    public RexNode getFunctionRexNode() {
        return functionRexNode;
    }

    @Override
    public void setFunctionRexNode(RexNode functionRexNode) {
        this.functionRexNode = functionRexNode;
    }

    @Override
    public List<RexNode> buildOutput() {
        return ImmutableList.copyOf(outputCols);
    }
}
