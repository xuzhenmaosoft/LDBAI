/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.evaluator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.datapps.linkoopdb.worker.spark.ai.core.AiEvaluator;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.evaluation.RankingMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

public class RankingEvaluator implements AiEvaluator, Serializable {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"precisionat", "meanaverageprecision", "ndcgat"};
    }

    public LogicalPlan precisionat(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        //predictionAndLabel groupBy(user) predictionItems Array double  lableItmes Array double
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        Dataset<Row> labelData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[1]);
        JavaRDD<Tuple2<List<Integer>, List<Integer>>> relevantDocs = transform(predictData, labelData);
        RankingMetrics<Integer> metrics = RankingMetrics.of(relevantDocs);
        int k = (Integer) arguments[2];
        double precisionAtK = metrics.precisionAt(k);
        List<Row> rows = Arrays.asList(RowFactory.create(precisionAtK));
        StructType schema = new StructType(new StructField[]{new StructField(
            "precisionat", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan meanaverageprecision(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        //predictionAndLabel groupBy(user) predictionItems Array double  lableItmes Array double
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        Dataset<Row> labelData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[1]);
        JavaRDD<Tuple2<List<Integer>, List<Integer>>> relevantDocs = transform(predictData, labelData);
        RankingMetrics<Integer> metrics = RankingMetrics.of(relevantDocs);
        double MAP = metrics.meanAveragePrecision();
        List<Row> rows = Arrays.asList(RowFactory.create(MAP));
        StructType schema = new StructType(new StructField[]{new StructField(
            "MAP", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    public LogicalPlan ndcgat(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        //predictionAndLabel groupBy(user) predictionItems Array double  lableItmes Array double
        Dataset<Row> predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        Dataset<Row> labelData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[1]);
        JavaRDD<Tuple2<List<Integer>, List<Integer>>> relevantDocs = transform(predictData, labelData);
        RankingMetrics<Integer> metrics = RankingMetrics.of(relevantDocs);
        int k = (Integer) arguments[2];
        double ndcgAt = metrics.ndcgAt(k);
        List<Row> rows = Arrays.asList(RowFactory.create(ndcgAt));
        StructType schema = new StructType(new StructField[]{new StructField(
            "ndcgat", DataTypes.DoubleType, false, Metadata.empty())});
        Dataset<Row> dataFrame = context.getSparkSession().createDataFrame(rows, schema);
        return dataFrame.logicalPlan();
    }

    private JavaRDD<Tuple2<List<Integer>, List<Integer>>> transform(Dataset<Row> predictData,
        Dataset<Row> labelData) {
        JavaRDD<Row> predictionJavaRDD = predictData.toJavaRDD();
        JavaPairRDD<Integer, List<Integer>> predictionUseridItemsid = predictionJavaRDD
            .mapToPair(new PairFunction<Row, Integer, List<Integer>>() {
                @Override
                public Tuple2<Integer, List<Integer>> call(Row row) throws Exception {
                    ArrayList<Integer> listInt = new ArrayList<>();
                    Integer userid = row.getAs("USERID");
                    Object recommendations = row.getAs("RECOMMENDATIONS");
                    ArrayList<ItemsId> listItems = new Gson()
                        .fromJson((String) recommendations, new TypeToken<ArrayList<ItemsId>>() {
                        }.getType());
                    for (int i = 0; i < listItems.size(); i++) {
                        listInt.add(listItems.get(i).getItemId());
                    }
                    return new Tuple2<Integer, List<Integer>>(userid, listInt);
                }
            });
        JavaRDD<Row> labelJavaRDD = labelData.toJavaRDD();
        JavaPairRDD<Integer, Integer> pairUseridItemsid = labelJavaRDD
            .mapToPair(new PairFunction<Row, Integer, Integer>() {
                @Override
                public Tuple2<Integer, Integer> call(Row row) throws Exception {
                    ArrayList<Integer> listInt = new ArrayList<>();
                    Integer userid = row.getAs("USERID");
                    Integer itemid = row.getAs("ITEMID");
                    return new Tuple2<Integer, Integer>(userid, itemid);
                }
            });
        JavaPairRDD<Integer, Iterable<Integer>> groupUseridItemsid = pairUseridItemsid
            .groupByKey();
        JavaPairRDD<Integer, List<Integer>> labelUseridItemsid = groupUseridItemsid
            .mapValues(new Function<Iterable<Integer>, List<Integer>>() {
                @Override
                public List<Integer> call(Iterable<Integer> integers) throws Exception {
                    ArrayList<Integer> listInt = new ArrayList<>();
                    integers.forEach(integer -> listInt.add(integer));
                    return listInt;
                }
            });
        return predictionUseridItemsid.join(labelUseridItemsid).values();
    }

    class ItemsId {

        int ITEMID;

        public int getItemId() {
            return ITEMID;
        }

        public void setItemId(int itemId) {
            this.ITEMID = itemId;
        }
    }

}
