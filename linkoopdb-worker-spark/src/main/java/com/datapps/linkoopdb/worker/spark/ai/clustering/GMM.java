package com.datapps.linkoopdb.worker.spark.ai.clustering;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DoubleType$;

/**
 * @author xingbu
 * @version 1.0 created by 2018/11/26 0026
 */
public class GMM extends SparkMLModelBase {

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            GaussianMixtureModel.load(path));
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"gaussian_mixture_train", "gaussian_mixture_predict"};
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new GaussianMixture()
            .setFeaturesCol("FEATURES")
            .setPredictionCol("PREDICTION");
    }

    @Override
    public Dataset<Row> processResult(Dataset<Row> transform) {
        return transform
            .withColumn("PREDICTION", transform.col("PREDICTION").cast(DoubleType$.MODULE$));
    }

}
