/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.classifier;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.sql.SparkSession;

public class MultilayerPerceptronClassifier extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"multilayer_perceptron_classifier_train",
            "multilayer_perceptron_classifier_predict"};
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            MultilayerPerceptronClassificationModel.load(path));
    }

    @Override
    public void setFittingParams(Object[] arguments, String[] parameters, PipelineStage pipelineStage) {
        for (int i = 1; i < parameters.length; i++) {
            if (arguments[i] != null && pipelineStage.hasParam(parameters[i])) {
                Object argument = arguments[i];
                if (parameters[i].equalsIgnoreCase("layers")) {
                    int length = ((Object[]) argument).length;
                    int[] arr = new int[length];
                    for (int j = 0; j < length; j++) {
                        arr[j] = (int) ((Object[]) argument)[j];
                    }
                    pipelineStage.set(parameters[i], arr);
                    continue;
                }
                pipelineStage.set(parameters[i], arguments[i]);
            }
        }
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.classification.MultilayerPerceptronClassifier()
            .setFeaturesCol("FEATURES").setLabelCol("LABEL").setPredictionCol("PREDICTION");
    }
}
