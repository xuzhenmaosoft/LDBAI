package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;

public interface LdbPipelineStageEntity {

    List<RexNode> getInputCols();

    List<RexNode> getOutputCols();

    void setInputCols(List<RexNode> inputCols);

    void setOutputCols(List<RexNode> outputCols);

    LdbPipelineStageEntity getStageEntity();

    RexNode getFunctionRexNode();

    void setFunctionRexNode(RexNode function);

}
