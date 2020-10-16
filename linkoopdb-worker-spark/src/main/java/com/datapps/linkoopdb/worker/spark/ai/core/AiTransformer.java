/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spark.ai.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * @author xingbu modified by xingbu 2019-02-18 17:10:12
 *
 *         具有数据转换功能的算法将实现此接口
 */
public interface AiTransformer extends AiPipelineStage, AiReader {

    @Override
    default LogicalPlan execute(SparkFunctionContext context) throws Exception {
        return transform(context);
    }

    /**
     * 通过训练好的模型，将输入的数据进行转换
     */
    LogicalPlan transform(SparkFunctionContext context) throws Exception;


    /**
     * 每个AI函数对于输入数据的处理接口，如：double array转vector
     *
     * @param rows 输入数据
     */
    Dataset processRows(Dataset rows);


}
