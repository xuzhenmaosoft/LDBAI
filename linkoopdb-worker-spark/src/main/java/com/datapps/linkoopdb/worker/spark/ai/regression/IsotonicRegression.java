package com.datapps.linkoopdb.worker.spark.ai.regression;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.regression.IsotonicRegressionModel;
import org.apache.spark.sql.SparkSession;

public class IsotonicRegression extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"isotonic_regression_train", "isotonic_regression_predict"};
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.regression.IsotonicRegression()
            .setFeaturesCol("FEATURES")
            .setLabelCol("LABEL")
            .setPredictionCol("PREDICTION");

    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            IsotonicRegressionModel.load(path));
    }
}
