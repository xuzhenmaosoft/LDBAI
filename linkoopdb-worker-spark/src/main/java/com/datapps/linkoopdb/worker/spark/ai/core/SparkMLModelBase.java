package com.datapps.linkoopdb.worker.spark.ai.core;

import org.apache.spark.ml.util.DatasetUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author xingbu
 * @version 1.0 <p> created by　19-2-18 下午4:43
 *
 *          sparkML库中的机器学习算法模型将继承该抽象类 该类的所有子类都支持机器学习预测功能
 */
public abstract class SparkMLModelBase implements SparkML, RealTimeTransformer {

    /**
     * 对输入数据处理的默认实现
     *
     * @param rows 输入数据
     */
    @Override
    public Dataset processRows(Dataset rows) {
        return rows.withColumn("FEATURES", DatasetUtils.columnToVector(rows, "FEATURES"));
    }

    @Override
    public Dataset select(Dataset<Row> transformedData) {
        return transformedData.select("PREDICTION");
    }

}
