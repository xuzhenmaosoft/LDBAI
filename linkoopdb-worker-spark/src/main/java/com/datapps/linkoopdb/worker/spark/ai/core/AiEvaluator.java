package com.datapps.linkoopdb.worker.spark.ai.core;

import java.lang.reflect.Method;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public interface AiEvaluator extends AiTableFunction {

    @Override
    default LogicalPlan execute(SparkFunctionContext context) throws Exception {
        String method = context.getFunctionName().toLowerCase();
        Class<? extends AiEvaluator> evaluator = this.getClass();
        Method evaluatorMethod = evaluator.getMethod(method, new Class[]{SparkFunctionContext.class});
        LogicalPlan result = (LogicalPlan) evaluatorMethod.invoke(this, new Object[]{context});
        return result;
    }

}
