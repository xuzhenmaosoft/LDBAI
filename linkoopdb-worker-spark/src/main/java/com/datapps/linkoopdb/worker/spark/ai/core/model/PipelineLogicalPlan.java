package com.datapps.linkoopdb.worker.spark.ai.core.model;

import com.datapps.linkoopdb.worker.spi.plan.core.LdbPipeline;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * @author xingbu
 * @version 1.0
 *
 * created by　19-6-26 下午2:30
 */
public class PipelineLogicalPlan extends ModelLogicPlan {

    private LdbPipeline pipeline;

    public PipelineLogicalPlan(LogicalPlan metadataLogicPlan, LdbPipeline pipeline) {
        super(metadataLogicPlan, null);
        this.pipeline = pipeline;
    }

    public LdbPipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(LdbPipeline pipeline) {
        this.pipeline = pipeline;
    }
}
