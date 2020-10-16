/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.clustering;

import java.util.ArrayList;

import com.datapps.linkoopdb.worker.spark.ai.core.MetaDataType;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamPair;
import org.apache.spark.ml.param.Params;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DoubleType$;

public class KMeans extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"kmeans_train", "kmeans_predict"};
    }

    @Override
    public ParamPair[] addExtraMetas(SparkFunctionContext context, Params params) {
        Vector[] vectors;
        if (params instanceof KMeansModel) {
            vectors = ((KMeansModel) params).clusterCenters();
        } else {
            vectors = ((BisectingKMeansModel) params).clusterCenters();
        }
        ArrayList<Object> centerslist = new ArrayList<>();
        for (int i = 0; i < vectors.length; i++) {
            centerslist.add(vectors[i]);
        }
        return new ParamPair[]{
            MetaDataType.buidParamPair("clusterCenters", "the value of clusterCenters",
                "LinkoopDB AI version")
        };
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.clustering.KMeans()
            .setFeaturesCol("FEATURES")
            .setPredictionCol("PREDICTION");
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            KMeansModel.load(path));
    }

    @Override
    public Dataset<Row> processResult(Dataset<Row> transform) {
        return transform
            .withColumn("PREDICTION", transform.col("PREDICTION").cast(DoubleType$.MODULE$));
    }

}
