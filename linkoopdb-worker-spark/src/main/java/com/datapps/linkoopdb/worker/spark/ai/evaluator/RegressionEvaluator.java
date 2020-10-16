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

public class RegressionEvaluator implements AiEvaluator {

    @Override
    public String version() {
        return "1.0.0";
    }

    public LogicalPlan mse(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        org.apache.spark.ml.evaluation.RegressionEvaluator regressionEvaluator = new org.apache.spark.ml.evaluation.RegressionEvaluator()
            .setPredictionCol("PREDICTION")
            .setLabelCol("LABEL")
            .setMetricName("mse");
        double evaluate = regressionEvaluator.evaluate(predictData);
        List<Row> rows = Arrays.asList(RowFactory.create(evaluate));
        StructType schema = new StructType(new StructField[]{new StructField(
            "mse", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan rmse(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        String[] parameters = context.getParameters();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        org.apache.spark.ml.evaluation.RegressionEvaluator regressionEvaluator = new org.apache.spark.ml.evaluation.RegressionEvaluator()
            .setPredictionCol("PREDICTION")
            .setLabelCol("LABEL")
            .setMetricName("rmse");
        double evaluate = regressionEvaluator.evaluate(predictData);
        List<Row> rows = Arrays.asList(RowFactory.create(evaluate));
        StructType schema = new StructType(new StructField[]{new StructField(
            "rmse", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan r2(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        String[] parameters = context.getParameters();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        org.apache.spark.ml.evaluation.RegressionEvaluator regressionEvaluator = new org.apache.spark.ml.evaluation.RegressionEvaluator()
            .setPredictionCol("PREDICTION")
            .setLabelCol("LABEL")
            .setMetricName("r2");
        double evaluate = regressionEvaluator.evaluate(predictData);
        List<Row> rows = Arrays.asList(RowFactory.create(evaluate));
        StructType schema = new StructType(new StructField[]{new StructField(
            "r2", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan mae(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        String[] parameters = context.getParameters();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        org.apache.spark.ml.evaluation.RegressionEvaluator regressionEvaluator = new org.apache.spark.ml.evaluation.RegressionEvaluator()
            .setPredictionCol("PREDICTION")
            .setLabelCol("LABEL")
            .setMetricName("mae");
        double evaluate = regressionEvaluator.evaluate(predictData);
        List<Row> rows = Arrays.asList(RowFactory.create(evaluate));
        StructType schema = new StructType(new StructField[]{new StructField(
            "mae", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    @Override
    public String[] methods() {
        return new String[]{"mse", "rmse", "r2", "mae"};
    }
}
