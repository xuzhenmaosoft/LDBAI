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

public class Normalizer extends SparkMLFeatureBase implements RealTimeTransformer {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public Dataset select(Dataset<Row> transformedData) {
        return transformedData.select("NORMFEATURES");
    }

    @Override
    public String[] methods() {
        return new String[]{"normalizer", "normalizer_transformer"};
    }

    @Override
    public Dataset processRows(Dataset rows) {
        return rows.withColumn("FEATURES", DatasetUtils.columnToVector(rows, "FEATURES"));
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.feature.Normalizer()
            .setInputCol("FEATURES")
            .setOutputCol("NORMFEATURES");
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            org.apache.spark.ml.feature.Normalizer.load(path));
    }
}
