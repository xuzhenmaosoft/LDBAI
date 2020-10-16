/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spark.ai.classifier;

import com.datapps.linkoopdb.worker.spark.ai.core.MetaDataType;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.param.ParamPair;
import org.apache.spark.ml.param.Params;
import org.apache.spark.sql.SparkSession;

public class NaiveBayes extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"naive_bayes_train", "naive_bayes_predict"};
    }

    @Override
    public ParamPair[] addExtraMetas(SparkFunctionContext context, Params params) {
        NaiveBayesModel dtm = (NaiveBayesModel) params;
        return new ParamPair[]{
            MetaDataType.buidParamPair("numClasses", "the number of classes", dtm.numClasses()),
            MetaDataType.buidParamPair("numFeatures", "the number of features", dtm.numClasses()),
        };
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.classification.NaiveBayes()
            .setLabelCol("LABEL")
            .setFeaturesCol("FEATURES").setPredictionCol("PREDICTION");
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            NaiveBayesModel.load(path));
    }

}
