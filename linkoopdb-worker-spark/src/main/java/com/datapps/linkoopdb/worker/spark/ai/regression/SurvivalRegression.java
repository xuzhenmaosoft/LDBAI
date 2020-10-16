package com.datapps.linkoopdb.worker.spark.ai.regression;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.regression.AFTSurvivalRegression;
import org.apache.spark.ml.regression.AFTSurvivalRegressionModel;
import org.apache.spark.sql.SparkSession;

public class SurvivalRegression extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"survival_regression_train", "survival_regression_predict"};
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new AFTSurvivalRegression()
            .setFeaturesCol("FEATURES")
            .setLabelCol("LABEL")
            .setPredictionCol("PREDICTION")
            .setCensorCol("CENSOR");
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            AFTSurvivalRegressionModel.load(path));
    }

    @Override
    public void setFittingParams(Object[] arguments, String[] parameters, PipelineStage pipelineStage) {
        for (int i = 0; i < parameters.length; i++) {
            if (arguments[i] != null && pipelineStage.hasParam(parameters[i])) {
                Object argument = arguments[i];
                if (argument instanceof Object[]) {
                    int length = ((Object[]) argument).length;
                    double[] arra = new double[length];
                    for (int j = 0; j < length; j++) {
                        arra[j] = Double.parseDouble((((Object[]) argument)[j]).toString());
                    }
                    pipelineStage.set(parameters[i], arra);
                    continue;
                }
                pipelineStage.set(parameters[i], arguments[i]);
            }
        }
    }
}
