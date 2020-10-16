package com.datapps.linkoopdb.worker.spark.ai.clustering;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.DistributedLDAModel;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.clustering.LocalLDAModel;
import org.apache.spark.sql.SparkSession;

/**
 * LDA  隐狄利克雷分布 文档主题聚类模型
 *
 * @author xingbu
 * @version 1.0 created by 2018/11/26 0026
 */
public class LDA extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        LDAModel model;
        try {
            model = DistributedLDAModel.load(path);
        } catch (Exception e) {
            model = LocalLDAModel.load(path);
        }
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            model);
    }

    @Override
    public String[] methods() {
        return new String[]{"lda_train", "lda_predict"};
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.clustering.LDA()
            .setFeaturesCol("FEATURES").setTopicDistributionCol("PREDICTION");
    }

}
