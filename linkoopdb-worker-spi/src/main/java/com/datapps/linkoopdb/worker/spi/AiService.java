package com.datapps.linkoopdb.worker.spi;

import java.util.List;
import java.util.Map;

/**
 * Created by gloway on 2018/12/18.
 *
 * @author xingbu modified on 2018/12/19
 */
public interface AiService {

    /**
     * Ai服务接口方法
     *
     * @param token 认证
     * @param functionName 函数名
     * @param paramMap 格式为:  参数名:参数值 , 如: "algorithm":"gbtclassifier_predict" 参数名均为小写,用下划线作为连接符,如:model_init_function
     * @param data 待预测数据集
     * @return 返回的数据 格式为 "data":List[Object[]] , "column":String[] Object[]为一行数据, 其中的每个元素与String[]中列名一一对应
     */
    Map streamingMLFunctionInvoke(String token, String functionName, Map<String, String> paramMap, List data) throws Exception;

}
