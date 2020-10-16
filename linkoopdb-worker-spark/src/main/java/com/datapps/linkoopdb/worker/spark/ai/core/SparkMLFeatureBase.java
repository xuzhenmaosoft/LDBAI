package com.datapps.linkoopdb.worker.spark.ai.core;

import org.apache.spark.sql.Dataset;

/**
 * @author xingbu
 * @version 1.0 <p> created by　19-2-19 上午10:10
 *
 *          sparkML库中的机器学习特征工程函数将继承该抽象类
 */
public abstract class SparkMLFeatureBase implements SparkML {

    /**
     * 对输入数据处理的默认实现
     *
     * @param rows 输入数据
     */
    @Override
    public Dataset processRows(Dataset rows) {
        return rows;
    }


}
