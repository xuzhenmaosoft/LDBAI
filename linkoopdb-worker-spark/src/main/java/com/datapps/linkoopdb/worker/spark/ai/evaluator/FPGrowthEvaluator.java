/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.evaluator;

import com.datapps.linkoopdb.worker.spark.ai.core.AiEvaluator;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class FPGrowthEvaluator implements AiEvaluator {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public LogicalPlan execute(SparkFunctionContext context) throws Exception {
        String method = context.getFunctionName();
        if (method.endsWith("FREQ")) {
            return support(context);
        } else if (method.endsWith("CONFIDENCE")) {
            return confidence(context);
        }
        return null;
    }

    /**
     * 把数据按照频繁度进行排序
     */
    public LogicalPlan support(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        ModelLogicPlan modelLogicPlan = (ModelLogicPlan) arguments[0];
        FPGrowthModel model = (FPGrowthModel) modelLogicPlan.getModel();
        Dataset<Row> freqItemsets = model.freqItemsets();
        return freqItemsets.logicalPlan();
    }

    public LogicalPlan confidence(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        ModelLogicPlan modelLogicPlan = (ModelLogicPlan) arguments[0];
        return ((FPGrowthModel) modelLogicPlan.getModel()).associationRules().logicalPlan();
    }

    @Override
    public String[] methods() {
        return new String[]{"fpgrowth_freq", "fpgrowth_confidence"};
    }
}

