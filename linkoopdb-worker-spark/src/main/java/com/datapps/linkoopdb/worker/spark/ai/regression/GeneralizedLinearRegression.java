package com.datapps.linkoopdb.worker.spark.ai.regression;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel;
import org.apache.spark.sql.SparkSession;

public class GeneralizedLinearRegression extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"glr_train", "glr_predict"};
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.regression.GeneralizedLinearRegression()
            .setFeaturesCol("FEATURES")
            .setLabelCol("LABEL")
            .setPredictionCol("PREDICTION");
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            GeneralizedLinearRegressionModel.load(path));
    }
}
