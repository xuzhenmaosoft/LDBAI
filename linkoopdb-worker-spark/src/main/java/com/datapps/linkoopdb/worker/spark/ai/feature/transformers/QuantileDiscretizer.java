/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.feature.transformers;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLFeatureBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.sql.SparkSession;

public class QuantileDiscretizer extends SparkMLFeatureBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"quantile_discretizer", "quantile_discretizer_transformer"};
    }

    @Override
    public void setFittingParams(Object[] arguments, String[] parameters, PipelineStage pipelineStage) {
        for (int i = 1; i < parameters.length; i++) {
            if (arguments[i] != null && pipelineStage.hasParam(parameters[i])) {
                Object argument = arguments[i];
                if (argument instanceof Object[]) {
                    if (parameters[i].equalsIgnoreCase("numBucketsArray")) {
                        int length = ((Object[]) argument).length;
                        int[] arra = new int[length];
                        for (int j = 0; j < length; j++) {
                            arra[j] = (int) ((Object[]) argument)[j];
                        }
                        pipelineStage.set(parameters[i], arra);
                        continue;
                    } else {
                        int length = ((Object[]) argument).length;
                        String[] arra = new String[length];
                        for (int j = 0; j < length; j++) {
                            arra[j] = ((Object[]) argument)[j].toString();
                        }
                        pipelineStage.set(parameters[i], arra);
                        continue;
                    }
                }
                pipelineStage.set(parameters[i], arguments[i]);
            }
        }
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.feature.QuantileDiscretizer();
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            Bucketizer.load(path));
    }
}
