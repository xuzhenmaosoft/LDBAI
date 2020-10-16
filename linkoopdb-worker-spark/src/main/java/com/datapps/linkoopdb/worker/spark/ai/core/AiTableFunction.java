package com.datapps.linkoopdb.worker.spark.ai.core;

import com.datapps.linkoopdb.worker.spi.LdbTableFunction;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public interface AiTableFunction extends LdbTableFunction {

    LogicalPlan execute(SparkFunctionContext context) throws Exception;

    @Override
    default Object invoke(Object context) throws Exception {
        return execute((SparkFunctionContext) context);
    }
}
