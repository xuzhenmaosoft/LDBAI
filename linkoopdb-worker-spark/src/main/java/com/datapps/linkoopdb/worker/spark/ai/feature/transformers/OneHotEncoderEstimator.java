/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.feature.transformers;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLFeatureBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.sql.SparkSession;

public class OneHotEncoderEstimator extends SparkMLFeatureBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"onehotencoder_estimator", "onehotencoder_estimator_transformer"};
    }

    @Override
    public void setFittingParams(Object[] arguments, String[] parameters, PipelineStage pipelineStage) {
        for (int i = 1; i < parameters.length; i++) {
            if (arguments[i] != null && pipelineStage.hasParam(parameters[i])) {
                Object argument = arguments[i];
                if (argument instanceof Object[]) {
                    int length = ((Object[]) argument).length;
                    String[] arra = new String[length];
                    for (int j = 0; j < length; j++) {
                        arra[j] = ((Object[]) argument)[j].toString();
                    }
                    pipelineStage.set(parameters[i], arra);
                    continue;
                }
                pipelineStage.set(parameters[i], arguments[i]);
            }
        }
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.feature.OneHotEncoder();
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            OneHotEncoderModel.load(path));
    }
}
