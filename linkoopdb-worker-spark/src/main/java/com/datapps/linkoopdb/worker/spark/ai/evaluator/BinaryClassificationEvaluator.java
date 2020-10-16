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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;
import scala.Tuple2;

public class BinaryClassificationEvaluator implements AiEvaluator, Serializable {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"precisionbythreshold", "recallbythreshold", "fmeasurebythreshold",
            "pr", "roc", "auprc", "auroc"};
    }

    public LogicalPlan auroc(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictData);
        double prc = metrics.areaUnderROC();
        List<Row> rows = Arrays.asList(RowFactory.create(prc));
        StructType schema = new StructType(new StructField[]{new StructField(
            "auroc", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan auprc(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictData);
        double prc = metrics.areaUnderPR();
        List<Row> rows = Arrays.asList(RowFactory.create(prc));
        StructType schema = new StructType(new StructField[]{new StructField(
            "auprc", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan roc(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        //sql 传入的data column 0：prediction(score) 1:label
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictData);
        JavaRDD<Tuple2<Object, Object>> roc = metrics.roc().toJavaRDD();
        JavaRDD<Row> rowData = roc.map(new Function<Tuple2<Object, Object>, Row>() {
            @Override
            public Row call(Tuple2<Object, Object> tuple) throws Exception {
                Row row = RowFactory
                    .create(new Double(tuple._1().toString()), new Double(tuple._2().toString()));
                return row;
            }
        });
        StructType schema = new StructType(new StructField[]{new StructField(
            "fpr", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("tpr", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rowData, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan pr(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictData);
        JavaRDD<Tuple2<Object, Object>> pr = metrics.pr().toJavaRDD();
        JavaRDD<Row> rowData = pr.map(new Function<Tuple2<Object, Object>, Row>() {
            @Override
            public Row call(Tuple2<Object, Object> tuple) throws Exception {
                Row row = RowFactory
                    .create(new Double(tuple._1().toString()), new Double(tuple._2().toString()));
                return row;
            }
        });
        StructType schema = new StructType(new StructField[]{new StructField(
            "recall", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("precision", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rowData, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan fmeasurebythreshold(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictData);
        double beta = (double) arguments[1];
        JavaRDD<Tuple2<Object, Object>> fmeasure = metrics.fMeasureByThreshold(beta).toJavaRDD();
        JavaRDD<Row> rowData = fmeasure.map(new Function<Tuple2<Object, Object>, Row>() {
            @Override
            public Row call(Tuple2<Object, Object> tuple) throws Exception {
                Row row = RowFactory
                    .create(new Double(tuple._1().toString()), new Double(tuple._2().toString()));
                return row;
            }
        });
        StructType schema = new StructType(new StructField[]{new StructField(
            "threshold", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("fmeasure", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rowData, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan recallbythreshold(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictData);
        JavaRDD<Tuple2<Object, Object>> recall = metrics.recallByThreshold().toJavaRDD();
        JavaRDD<Row> rowData = recall.map(new Function<Tuple2<Object, Object>, Row>() {
            @Override
            public Row call(Tuple2<Object, Object> tuple) throws Exception {
                Row row = RowFactory
                    .create(new Double(tuple._1().toString()), new Double(tuple._2().toString()));
                return row;
            }
        });
        StructType schema = new StructType(new StructField[]{new StructField(
            "threshold", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("recall", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rowData, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan precisionbythreshold(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictData);
        JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();
        JavaRDD<Row> rowData = precision.map(new Function<Tuple2<Object, Object>, Row>() {
            @Override
            public Row call(Tuple2<Object, Object> tuple) throws Exception {
                Row row = RowFactory
                    .create(new Double(tuple._1().toString()), new Double(tuple._2().toString()));
                return row;
            }
        });
        StructType schema = new StructType(new StructField[]{new StructField(
            "threshold", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("precision", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rowData, schema);
        return dataFrame.logicalPlan();
    }

}
