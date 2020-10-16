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
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.param.ParamPair;
import org.apache.spark.ml.param.Params;
import org.apache.spark.sql.SparkSession;

public class DecisionTree extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"decision_tree_train", "decision_tree_predict"};
    }


    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            DecisionTreeClassificationModel.load(path));
    }

    @Override
    public ParamPair[] addExtraMetas(SparkFunctionContext context, Params params) {
        DecisionTreeClassificationModel dtm = (DecisionTreeClassificationModel) params;
        return new ParamPair[]{
            MetaDataType.buidParamPair("numClasses", "the number of classes", dtm.numClasses()),
            MetaDataType.buidParamPair("numFeatures", "the number of features", dtm.numFeatures()),
            MetaDataType.buidParamPair("numNodes", "the number of nodes", dtm.numNodes())
        };
    }


    @Override
    public PipelineStage createSimplePipelineStage() {
        return new DecisionTreeClassifier()
            .setLabelCol("LABEL")
            .setFeaturesCol("FEATURES")
            .setPredictionCol("PREDICTION");
    }

}
