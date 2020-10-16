/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.regression;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.sql.SparkSession;

public class DecisionTreeRegressor extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"decision_tree_regression_train", "decision_tree_regression_predict"};
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            DecisionTreeRegressionModel.load(path));
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.regression.DecisionTreeRegressor()
            .setFeaturesCol("FEATURES")
            .setLabelCol("LABEL")
            .setPredictionCol("PREDICTION");
    }

}
