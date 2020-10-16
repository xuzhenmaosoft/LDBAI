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
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.util.DatasetUtils;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class KMeansEvaluator implements AiEvaluator {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public LogicalPlan execute(SparkFunctionContext context) throws Exception {
        String method = context.getFunctionName();
        if (method.endsWith("WSSSE")) {
            return wssse(context);
        }
        return null;
    }

    public LogicalPlan wssse(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        ModelLogicPlan modelLogicPlan = (ModelLogicPlan) arguments[0];
        Dataset data = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[1]);
        data = data.withColumn("FEATURES", DatasetUtils.columnToVector(data, "FEATURES"));

        double cost;

        if (modelLogicPlan.getModel() instanceof KMeansModel) {
            KMeansModel model = (KMeansModel) modelLogicPlan.getModel();
            org.apache.spark.mllib.clustering.KMeansModel kMeansModel = model.parentModel();
            RDD<Vector> vectorRDD = DatasetUtils.columnToOldVector(data, model.getFeaturesCol());
            cost = kMeansModel.computeCost(vectorRDD);
        } else {
            cost = ((BisectingKMeansModel) modelLogicPlan.getModel()).computeCost(data);
        }

        List<Row> rows = Arrays.asList(RowFactory.create(cost));
        StructType schema = new StructType(new StructField[]{new StructField(
            "wssse", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    @Override
    public String[] methods() {
        return new String[]{"kmeans_wssse", "bisecting_kmeans_wssse"};
    }

}
