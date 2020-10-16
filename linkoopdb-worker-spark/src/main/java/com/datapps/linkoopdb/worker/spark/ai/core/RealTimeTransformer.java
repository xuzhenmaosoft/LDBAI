package com.datapps.linkoopdb.worker.spark.ai.core;

import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author xingbu 2019-02-20 12:04:02
 *
 *         实时转换(预测)服务的接口，具有此功能的类一定是具有转换功能的，故继承自AiTransformer
 */
public interface RealTimeTransformer extends AiTransformer {

    /**
     * ai实时转换功能
     * @param model 加载后的模型
     * @param transformData 输入的数据
     * @return  进行转换后的数据
     */
    default Dataset transform(ModelLogicPlan model, Dataset transformData) {
        transformData = processStreamingRow(transformData);
        Dataset<Row> transformedData = model.getModel().transform(transformData);
        return this.select(transformedData);
    }

    /**
     * 对转换后的数据进行进一步处理，只保留需要输出的列
     * 该功能没有默认实现，需要在子类中根据算法的特性进行实现
     */
    Dataset select(Dataset<Row> transformedData);


    /**
     * 对实时转换时输入数据的转换方法，默认与批时相同
     */
    default Dataset processStreamingRow(Dataset rows) {
        return processRows(rows);
    }

}
