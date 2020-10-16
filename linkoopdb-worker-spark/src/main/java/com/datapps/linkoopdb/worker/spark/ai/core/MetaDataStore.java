package com.datapps.linkoopdb.worker.spark.ai.core;

import org.apache.spark.ml.param.Params;
import org.apache.spark.sql.Dataset;

/**
 * 具有元数据存储功能的函数将实现此接口
 *
 * @author xingbu 2019-02-18 16:47:47
 */
public interface MetaDataStore {

    /**
     * 创建元数据的方法
     *
     * @param context 函数上下文，用来获取执行某个函数时由用户输入的函数
     * @param params 包含元数据信息的对象，如：图对象、训练后的模型
     */
    Dataset<MetaDataType> buildMetaData(SparkFunctionContext context, Params params);

}
