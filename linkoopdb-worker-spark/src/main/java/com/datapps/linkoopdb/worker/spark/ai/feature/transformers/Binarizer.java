/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.feature.transformers;

import com.datapps.linkoopdb.worker.spark.ai.core.RealTimeTransformer;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLFeatureBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.util.DatasetUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Binarizer extends SparkMLFeatureBase implements RealTimeTransformer {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public Dataset select(Dataset<Row> transformedData) {
        return transformedData.select("BINARIZEDFEATURE");
    }

    @Override
    public String[] methods() {
        return new String[]{"binarizer", "binarizer_transformer"};
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.feature.Binarizer();
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            org.apache.spark.ml.feature.Binarizer.load(path));
    }

    @Override
    public Dataset processRows(Dataset rows) {
        return rows.withColumn("FEATURES", DatasetUtils.columnToVector(rows, "FEATURES"));
    }

    @Override
    public void setFittingParams(Object[] arguments, String[] parameters, PipelineStage pipelineStage) {
        for (int i = 0; i < parameters.length; i++) {
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
}
