/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spark.ai.core;

import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * @author xingbu modified by xingbu 2019-02-18 17:09:07
 *
 *         具有训练功能的算法将实现此接口
 */

public interface AiEstimator extends AiPipelineStage {

    @Override
    default LogicalPlan execute(SparkFunctionContext context) throws Exception {
        return fit(context);
    }

    /**
     * 模型训练
     */
    ModelLogicPlan fit(SparkFunctionContext context) throws Exception;

}
