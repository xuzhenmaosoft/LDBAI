/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.evaluator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.datapps.linkoopdb.worker.spark.ai.core.AiEvaluator;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MulticlassEvaluator implements AiEvaluator {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"accuracy", "weightedfmeasure", "weightedrecall", "weightedprecision",
            "fprbylabel", "weightedfpr", "fmeasurebylabel", "precisionbylabel", "recallbylabel",
            "confusionmatrix"};
    }

    public LogicalPlan confusionmatrix(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MulticlassMetrics metrics = new MulticlassMetrics(predictData);
        Matrix matrix = metrics.confusionMatrix();
        int rows = matrix.numRows();
        int cols = matrix.numCols();
        ArrayList<Row> rowList = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            ArrayList<Double> alDouble = new ArrayList<>();
            for (int j = 0; j < cols; j++) {
                double value = matrix.apply(i, j);
                alDouble.add(value);
            }
            rowList.add(RowFactory.create(alDouble));
        }
        StructType schema = new StructType(new StructField[]{new StructField(
            "confusionmatrix", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rowList, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan accuracy(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MulticlassMetrics metrics = new MulticlassMetrics(predictData);
        double accuracy = metrics.accuracy();
        List<Row> rows = Arrays.asList(RowFactory.create(accuracy));
        StructType schema = new StructType(new StructField[]{new StructField(
            "accuracy", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan weightedfmeasure(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MulticlassMetrics metrics = new MulticlassMetrics(predictData);
        double beta = (double) arguments[1];
        double weightedFMeasure = metrics.weightedFMeasure(beta);
        List<Row> rows = Arrays.asList(RowFactory.create(weightedFMeasure));
        StructType schema = new StructType(new StructField[]{new StructField(
            "weightedfmeasure", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan weightedrecall(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MulticlassMetrics metrics = new MulticlassMetrics(predictData);
        double weightedRecall = metrics.weightedRecall();
        List<Row> rows = Arrays.asList(RowFactory.create(weightedRecall));
        StructType schema = new StructType(new StructField[]{new StructField(
            "weightedrecall", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan weightedprecision(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MulticlassMetrics metrics = new MulticlassMetrics(predictData);
        double weightedPrecision = metrics.weightedPrecision();
        List<Row> rows = Arrays.asList(RowFactory.create(weightedPrecision));
        StructType schema = new StructType(new StructField[]{new StructField(
            "weightedprecision", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan fprbylabel(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MulticlassMetrics metrics = new MulticlassMetrics(predictData);
        double label = (double) arguments[1];
        double falsePositiveRate = metrics.falsePositiveRate(label);
        List<Row> rows = Arrays.asList(RowFactory.create(falsePositiveRate));
        StructType schema = new StructType(new StructField[]{new StructField(
            "fprbylabel", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan weightedfpr(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MulticlassMetrics metrics = new MulticlassMetrics(predictData);
        double weightedFalsePositiveRate = metrics.weightedFalsePositiveRate();
        List<Row> rows = Arrays.asList(RowFactory.create(weightedFalsePositiveRate));
        StructType schema = new StructType(new StructField[]{new StructField(
            "weightedfpr", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan fmeasurebylabel(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MulticlassMetrics metrics = new MulticlassMetrics(predictData);
        double label = (double) arguments[1];
        double beta = (double) arguments[2];
        double fmeasure = metrics.fMeasure(label, beta);
        List<Row> rows = Arrays.asList(RowFactory.create(fmeasure));
        StructType schema = new StructType(new StructField[]{new StructField(
            "fmeasurebylabel", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan precisionbylabel(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MulticlassMetrics metrics = new MulticlassMetrics(predictData);
        double label = (double) arguments[1];
        double precision = metrics.precision(label);
        List<Row> rows = Arrays.asList(RowFactory.create(precision));
        StructType schema = new StructType(new StructField[]{new StructField(
            "precisionbylabel", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan recallbylabel(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MulticlassMetrics metrics = new MulticlassMetrics(predictData);
        double label = (double) arguments[1];
        double recall = metrics.recall(label);
        List<Row> rows = Arrays.asList(RowFactory.create(recall));
        StructType schema = new StructType(new StructField[]{new StructField(
            "recallbylabel", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

}
