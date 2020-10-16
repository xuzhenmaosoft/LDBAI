/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.classifier;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.sql.SparkSession;

public class RandomForestClassifier extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"random_forest_classifier_train", "random_forest_classifier_predict"};
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            RandomForestClassificationModel.load(path));
    }


    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.classification.RandomForestClassifier().setLabelCol("LABEL")
            .setFeaturesCol("FEATURES")
            .setPredictionCol("PREDICTION");
    }
}
