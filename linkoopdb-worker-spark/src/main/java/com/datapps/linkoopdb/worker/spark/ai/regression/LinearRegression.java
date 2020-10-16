/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.regression;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.SparkSession;

public class LinearRegression extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"linear_regression_train", "linear_regression_predict"};
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            LinearRegressionModel.load(path));
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.regression.LinearRegression()
            .setFeaturesCol("FEATURES")
            .setLabelCol("LABEL")
            .setPredictionCol("PREDICTION");
    }
}
