/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class FPGrowth extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"fpgrowth_train"};
    }


    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            FPGrowthModel.load(path));
    }

    @Override
    public Dataset processRows(Dataset rows) {
        return rows;
    }

    /**
     * 该算法不需要实现此功能
     */
    @Override
    public LogicalPlan transform(SparkFunctionContext context) throws Exception {
        return null;
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.fpm.FPGrowth()
            .setItemsCol("ITEMS").setPredictionCol("PREDICTION");
    }

}
