/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.evaluator;

import java.util.Arrays;
import java.util.List;

import com.datapps.linkoopdb.worker.spark.ai.core.AiEvaluator;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ClusteringEvaluator implements AiEvaluator {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"silhouettescore"};
    }

    public LogicalPlan silhouettescore(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        org.apache.spark.ml.evaluation.ClusteringEvaluator metrics = new org.apache.spark.ml.evaluation.ClusteringEvaluator()
            .setPredictionCol("PREDICTION")
            .setFeaturesCol("FEATURES")
            .setMetricName("silhouette");
        double silhouette = metrics.evaluate(predictData);
        List<Row> rows = Arrays.asList(RowFactory.create(silhouette));
        StructType schema = new StructType(new StructField[]{new StructField(
            "silhouettescore", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

}
