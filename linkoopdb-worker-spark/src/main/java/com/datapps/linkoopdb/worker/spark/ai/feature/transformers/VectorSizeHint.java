/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.feature.transformers;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLFeatureBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.util.DatasetUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class VectorSizeHint extends SparkMLFeatureBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"vector_size_hint", "vector_size_hint_transformer"};
    }

    @Override
    public Dataset processRows(Dataset rows) {
        return rows.withColumn("USERFEATURES", DatasetUtils.columnToVector(rows, "USERFEATURES"));
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.feature.VectorSizeHint()
            .setInputCol("USERFEATURES");
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            org.apache.spark.ml.feature.VectorSizeHint.load(path));
    }
}
