/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spark.ai.core;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public interface MLModel extends AiEstimator, AiTransformer {

    @Override
    default LogicalPlan execute(SparkFunctionContext context) throws Exception {
        String functionName = context.getFunctionName();
        if (functionName.endsWith("_TRAIN") || functionName.endsWith("_TRANSFORMER") || functionName.startsWith("LOAD_")) {
            return fit(context);
        }
        return transform(context);
    }

}
