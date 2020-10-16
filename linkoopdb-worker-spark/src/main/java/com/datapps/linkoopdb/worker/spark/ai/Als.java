/**
 * This file is part of linkoopdb. <p> Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spark.ai;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_json;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;


public class Als extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"als_train", "als_recommend", "als_predict", "als_item_user_factors"};
    }


    @Override
    public LogicalPlan execute(SparkFunctionContext context) throws Exception {
        String method = context.getFunctionName();
        if (method.equalsIgnoreCase("als_train")) {
            return fit(context);
        } else if (method.equalsIgnoreCase("als_recommend")) {
            return recommend(context);
        } else if (method.equalsIgnoreCase("als_predict")) {
            return transform(context);
        } else if (method.equalsIgnoreCase("als_item_user_factors")) {
            return alsFactors(context);
        }
        return null;
    }

    public LogicalPlan alsFactors(SparkFunctionContext context) {
        Object[] arguments = context.getArguments();
        ModelLogicPlan modelLogicPlan = (ModelLogicPlan) arguments[0];
        String attr = (String) arguments[1];
        if (attr.equalsIgnoreCase("user")) {
            return ((ALSModel) modelLogicPlan.getModel()).userFactors()
                .logicalPlan();
        } else if (attr.equalsIgnoreCase("item")) {
            return ((ALSModel) modelLogicPlan.getModel()).userFactors()
                .logicalPlan();
        } else {
            throw new IllegalArgumentException(
                "attr parameter must be 'user' or 'item' but get '" + attr + "'");
        }
    }

    @Override
    public Dataset processRows(Dataset rows) {
        return rows;
    }

    public LogicalPlan recommend(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        ModelLogicPlan modelLogicPlan = (ModelLogicPlan) arguments[0];
        Dataset data = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[1]);
        return ((ALSModel) modelLogicPlan.getModel())
            .recommendForUserSubset(data, (Integer) arguments[2])
            .select(col("USERID"), to_json(col("recommendations")).as("recommendations"))
            .logicalPlan();
    }

    @Override
    public ModelLogicPlan load(SparkSession sparkSession, String alias, String path) {
        return new ModelLogicPlan(sparkSession.read().parquet(path).alias(alias).logicalPlan(),
            ALSModel.load(path));
    }


    @Override
    public void setFittingParams(Object[] arguments, String[] parameters,
        PipelineStage pipelineStage) {
        super.setFittingParams(arguments, parameters, pipelineStage);
        ALS als = (ALS) pipelineStage;
        als.setColdStartStrategy("drop");
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        org.apache.spark.ml.recommendation.ALS als = new org.apache.spark.ml.recommendation.ALS();
        als.setUserCol("USERID")
            .setItemCol("ITEMID")
            .setRatingCol("RATING")
            .setPredictionCol("PREDICTION");
        return als;
    }
}
