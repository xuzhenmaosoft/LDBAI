package com.datapps.linkoopdb.worker.spark.ai.core;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamPair;
import org.apache.spark.ml.param.Params;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.ml.param.shared.HasOutputCols;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * @author xingbu
 * @version 1.0 <p> created by　19-2-18 下午4:30
 */
interface SparkML extends MLModel {


    /**
     * 模型预测的默认实现
     */
    @Override
    default LogicalPlan transform(SparkFunctionContext context) throws Exception {
        Object[] arguments = context.getArguments();
        ModelLogicPlan modelLogicPlan = (ModelLogicPlan) arguments[0];
        Dataset predictData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[1]);
        predictData = processRows(predictData);
        Dataset<Row> transform = modelLogicPlan.getModel().transform(predictData);
        transform = processResult(transform);
        return transform.logicalPlan();
    }


    /**
     * 对结果进行处理的默认实现 目前是为了解决几个聚类算法返回结果为Integer的情况
     */
    default Dataset<Row> processResult(Dataset<Row> transform) {
        return transform;
    }

    /**
     * 设置属性的默认实现
     */
    default void setFittingParams(Object[] arguments, String[] parameters,
        PipelineStage pipelineStage) {
        for (int i = pipelineStage instanceof Estimator ? 1 : 0; i < parameters.length; i++) {
            if (arguments[i] != null && pipelineStage.hasParam(parameters[i])) {
                pipelineStage.set(parameters[i], arguments[i]);
            }
        }
    }

    /**
     * sparkML训练模型的默认实现 模板设计模式：在本类中实现逻辑的控制，具体的逻辑在其子类中进行实现 并且给出了常用的实现细节 模型训练分为以下几步： 1.获取传入的训练数据 2.创建模型并训练模型 3.创建存储元数据
     */
    @Override
    default ModelLogicPlan fit(SparkFunctionContext context) throws Exception {
        // 判断训练模型时是否需要输入的数据，如果需要数据则执行以下操作
        Transformer fit = processFit(context);
        Dataset<MetaDataType> metaData = buildMetaData(context, fit);
        return new ModelLogicPlan(metaData.logicalPlan(), fit);
    }

    /**
     * 训练模型过程中核心逻辑的默认实现
     */
    default Transformer processFit(SparkFunctionContext context) {
        PipelineStage pipelineStage = createSimplePipelineStage();
        Object[] arguments = context.getArguments();
        String[] parameters = context.getParameters();
        setFittingParams(arguments, parameters, pipelineStage);
        if (this instanceof SparkMLFeatureBase) {
            if (pipelineStage instanceof HasInputCol && context.getInputCols() != null) {
                Param<Object> inputColParam = pipelineStage.getParam("inputCol");
                boolean inputColIsEmpty =  pipelineStage.paramMap().get(inputColParam).isEmpty();
                if (!inputColIsEmpty) {
                    ((HasInputCol) pipelineStage).set("inputCol", context.getInputCols().get(0));
                }
            }
            if (pipelineStage instanceof HasOutputCol && context.getOutputCols() != null) {
                Param<Object> outputColParam = pipelineStage.getParam("outputCol");
                boolean outputColIsEmpty =  pipelineStage.paramMap().get(outputColParam).isEmpty();
                if (!outputColIsEmpty) {
                    ((HasOutputCol) pipelineStage).set("outputCol", context.getOutputCols().get(0));
                }
            }
            if (pipelineStage instanceof HasInputCols && context.getInputCols() != null) {
                Param<Object> inputColsParam = pipelineStage.getParam("inputCols");
                boolean inputColsIsEmpty =  pipelineStage.paramMap().get(inputColsParam).isEmpty();
                if (!inputColsIsEmpty) {
                    ((HasInputCols) pipelineStage).set("inputCols", context.getInputCols().toArray(new String[context.getInputCols().size()]));
                }
            }
            if (pipelineStage instanceof HasOutputCols && context.getOutputCols() != null) {
                Param<Object> outputColsParam = pipelineStage.getParam("outputCols");
                boolean outputColsIsEmpty =  pipelineStage.paramMap().get(outputColsParam).isEmpty();
                if (!outputColsIsEmpty) {
                    ((HasOutputCols) pipelineStage).set("outputCols", context.getOutputCols().toArray(new String[context.getOutputCols().size()]));
                }
            }
        }
        return pipelineStage instanceof Estimator ? (Transformer) ((Estimator) pipelineStage)
            .fit(processRows(context.getTrainset())) : (Transformer) pipelineStage;
    }

    /**
     * 给出的创建元数据的通用实现
     *
     * @param context 执行函数时的上下文
     * @param params 训练后的模型，用于提取其训练后的参数
     */
    @Override
    default Dataset<MetaDataType> buildMetaData(SparkFunctionContext context, Params params) {
        ParamPair[] defaultParamPairs = new ParamPair[]{
            MetaDataType.buidParamPair("ldbAiVersion", "LinkoopDB AI version", version()),
            MetaDataType.buidParamPair("updateTime", "last update time",
                new SimpleDateFormat("yyyy-MM-dd " + "HH:mm:ss").format(new Date()))};
        ParamPair[] extraMetas = addExtraMetas(context, params);
        ParamPair[] pairs = new ParamPair[extraMetas.length + defaultParamPairs.length];
        for (int i = 0; i < extraMetas.length; i++) {
            pairs[i] = extraMetas[i];
        }
        for (int i = 0; i < defaultParamPairs.length; i++) {
            pairs[i + extraMetas.length] = defaultParamPairs[i];
        }
        return MetaDataType.buildDataset(context.getSparkSession(), params, pairs);
    }

    /**
     * 给出额外的元数据
     *
     * @param context 执行函数时的上下文
     * @param params 训练后的模型，用于提取其训练后的参数
     */
    default ParamPair[] addExtraMetas(SparkFunctionContext context, Params params) {
        return new ParamPair[0];
    }

    /**
     * 创建简单转换模型的方法
     */
    PipelineStage createSimplePipelineStage();

}
