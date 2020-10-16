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
import org.apache.spark.mllib.evaluation.MultilabelMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MultilabelEvaluator implements AiEvaluator {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"mlprecision", "mlprecisionbylabel", "mlrecall", "mlrecallbylabel", "mlaccuracy",
            "mlfmeasure", "hammingloss", "microfmeasure", "microprecision", "microrecall",
            "subsetaccuracy", "mlfmeasurebylabel"};
    }

    public LogicalPlan mlprecision(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        double precision = metrics.precision();
        List<Row> rows = Arrays.asList(RowFactory.create(precision));
        StructType schema = new StructType(new StructField[]{new StructField(
            "mlprecision", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan mlprecisionbylabel(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        double label = (double) arguments[1];
        double precision = metrics.precision(label);
        List<Row> rows = Arrays.asList(RowFactory.create(precision));
        StructType schema = new StructType(new StructField[]{new StructField(
            "mlprecisionbylabel", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan mlrecall(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        double recall = metrics.recall();
        List<Row> rows = Arrays.asList(RowFactory.create(recall));
        StructType schema = new StructType(new StructField[]{new StructField(
            "mlrecall", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan mlrecallbylabel(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        double label = (double) arguments[1];
        double recall = metrics.recall(label);
        List<Row> rows = Arrays.asList(RowFactory.create(recall));
        StructType schema = new StructType(new StructField[]{new StructField(
            "mlrecallbylabel", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan mlaccuracy(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        double accuracy = metrics.accuracy();
        List<Row> rows = Arrays.asList(RowFactory.create(accuracy));
        StructType schema = new StructType(new StructField[]{new StructField(
            "mlaccuracy", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan mlfmeasure(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        double f1measure = metrics.f1Measure();
        List<Row> rows = Arrays.asList(RowFactory.create(f1measure));
        StructType schema = new StructType(new StructField[]{new StructField(
            "mlfmeasure", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan mlfmeasurebylabel(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        double label = (double) arguments[1];
        double f1measure = metrics.f1Measure(label);
        List<Row> rows = Arrays.asList(RowFactory.create(f1measure));
        StructType schema = new StructType(new StructField[]{new StructField(
            "mlfmeasurebylabel", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan hammingloss(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        //加权平均
        double hammingloss = metrics.hammingLoss();
        List<Row> rows = Arrays.asList(RowFactory.create(hammingloss));
        StructType schema = new StructType(new StructField[]{new StructField(
            "hammingloss", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan microfmeasure(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        //微平均fmeasure
        double microf1measure = metrics.microF1Measure();
        List<Row> rows = Arrays.asList(RowFactory.create(microf1measure));
        StructType schema = new StructType(new StructField[]{new StructField(
            "microfmeasure", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan microprecision(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        //微平均fmeasure
        double microprecision = metrics.microPrecision();
        List<Row> rows = Arrays.asList(RowFactory.create(microprecision));
        StructType schema = new StructType(new StructField[]{new StructField(
            "microprecision", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan microrecall(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        //微平均fmeasure
        double microrecall = metrics.microRecall();
        List<Row> rows = Arrays.asList(RowFactory.create(microrecall));
        StructType schema = new StructType(new StructField[]{new StructField(
            "microrecall", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan subsetaccuracy(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        MultilabelMetrics metrics = new MultilabelMetrics(predictData);
        //微平均fmeasure
        double subsetaccuracy = metrics.subsetAccuracy();
        List<Row> rows = Arrays.asList(RowFactory.create(subsetaccuracy));
        StructType schema = new StructType(new StructField[]{new StructField(
            "subsetaccuracy", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

}
