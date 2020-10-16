package com.datapps.linkoopdb.worker.spark.ai.core;

import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.sql.SparkSession;

/**
 * @author xingbu 2019-02-20 14:09:07
 *
 *         具有模型读取功能的
 */
public interface AiReader {

    /**
     * 模型加载
     *
     * @param sparkSession sparkSession
     * @param alias 模型的别名
     * @param path 模型路径
     */
    ModelLogicPlan load(SparkSession sparkSession, String alias, String path);
}
