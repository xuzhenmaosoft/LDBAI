package com.datapps.linkoopdb.worker.spark.ai.clustering;

import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DoubleType$;

/**
 * @author xingbu
 * @version 1.0 created by 2018/11/26 0026
 */
public class BisectingKMeans extends KMeans {

    @Override
    public String version() {
        return "1.0.0";
    }


    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            BisectingKMeansModel.load(path));
    }

    @Override
    public String[] methods() {
        return new String[]{"bisecting_kmeans_train", "bisecting_kmeans_predict"};
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.clustering.BisectingKMeans()
            .setFeaturesCol("FEATURES")
            .setPredictionCol("PREDICTION");
    }

    @Override
    public Dataset<Row> processResult(Dataset<Row> transform) {
        return transform
            .withColumn("PREDICTION", transform.col("PREDICTION").cast(DoubleType$.MODULE$));
    }

}
