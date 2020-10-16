package com.datapps.linkoopdb.worker.spark.ai;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

import com.datapps.linkoopdb.worker.spark.ai.core.MetaDataType;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.param.BooleanParam;
import org.apache.spark.ml.param.DoubleParam;
import org.apache.spark.ml.param.IntParam;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.ParamPair;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.ml.util.DatasetUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * 通过划分验证集和训练集进行模型超参数的搜索
 * <p>
 * 目前支持的模型:
 * 有监督学习:
 * 分类:GBTClassifier  RandomForestClassifier LogisticRegression DecisionTreeClassifier
 * 回归:LinearRegression
 * <p>
 * 支持的评估器:
 * binaryClassificationEvaluator,multiClassificationEvaluator
 * regressionEvaluator
 *
 * @author xingbu
 * @version 1.0 created by 2018/11/26 0026
 */
public class TrainValidationSplit extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"train_validation_split", "train_validation_predict"};
    }


    @Override
    public LogicalPlan execute(SparkFunctionContext context) throws Exception {
        String method = context.getFunctionName();

        if ("train_validation_split".equalsIgnoreCase(method)) {
            return fit(context);
        } else if ("train_validation_predict".equalsIgnoreCase(method)) {
            return transform(context);
        }
        return null;
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            TrainValidationSplitModel.load(path));
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return null;
    }

    @Override
    public Dataset processRows(Dataset rows) {
        rows = rows.withColumn("FEATURES", DatasetUtils.columnToVector(rows, "FEATURES"));
        rows = rows.withColumnRenamed("FEATURES", "features");
        rows = rows.withColumnRenamed("LABEL", "label");
        return rows;
    }

    @Override
    public ModelLogicPlan fit(SparkFunctionContext context) throws Exception {
        String[] parameters = context.getParameters();
        Object[] arguments = context.getArguments();
        // 原始数据集
        Dataset trainData = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[0]);
        trainData = processRows(trainData);
        // 获取模型训练的4个参数
        String estimator = null;
        String paramMapJson = null;
        Gson gson = new Gson();

        String evaluator = null;
        double trainRatio = 0.75;

        for (int i = 1; i < parameters.length; i++) {
            String key = parameters[i];
            if ("estimator".equalsIgnoreCase(key)) {
                estimator = (String) arguments[i];
            }
            if ("estimatorParamMaps".equalsIgnoreCase(key)) {
                paramMapJson = (String) arguments[i];
            }
            if ("evaluator".equalsIgnoreCase(key)) {
                evaluator = (String) arguments[i];
            }

            if ("trainRatio".equalsIgnoreCase(key)) {
                trainRatio = (double) arguments[i];
            }
        }
        Map<String, Object> estimatorParamMaps = gson.fromJson(paramMapJson, Map.class);

        org.apache.spark.ml.tuning.TrainValidationSplit trainValidationSplit = new TrainValidationSplitFactory(estimator, estimatorParamMaps, evaluator,
            trainRatio).getInstance();

        // 用户输入的值的集合
        Set<String> paramSet = estimatorParamMaps.keySet();

        // 设置收集训练中的模型
        trainValidationSplit.setCollectSubModels(true);

        TrainValidationSplitModel fit = trainValidationSplit.fit(trainData);

        // 获取最优模型
        Model<?> bestModel = fit.bestModel();

        // 获取训练中其他的模型
        Model<?>[] subModels = fit.subModels();

        // 每个模型对应的打分
        double[] validationRatios = fit.validationMetrics();

        List<Map> resultList = new ArrayList<>(subModels.length);

        String validation = "validation";

        for (int i = 0; i < subModels.length; i++) {
            Model<?> subModel = subModels[i];
            Map resultMap = new HashMap<>();
            for (String paramName : paramSet) {
                Param<Object> param = subModel.getParam(paramName);
                Option<Object> option = subModel.get(param);
                Object value = option.get();
                resultMap.put(paramName, value);
            }
            resultMap.put(validation, validationRatios[i]);
            resultList.add(resultMap);
        }

        if ("REGRESSIONEVALUATOR".equals(evaluator.toUpperCase())) {
            // 如果是回归问题 按照误差的大小升序排列 rmse
            Collections.sort(resultList, (o1, o2) -> ((double) o1.get(validation) >= (double) o2.get(validation)) ? 1 : -1);
        } else {
            // 如果是分类问题 按照预测的准确率的大小升序排列 二分类:areaUnderROC 多分类:f1
            Collections.sort(resultList, (o1, o2) -> ((double) o1.get(validation) >= (double) o2.get(validation)) ? -1 : 1);

        }

        String validationMetricsJson = gson.toJson(resultList);

        List<ParamPair<?>> paramPairs = JavaConversions.seqAsJavaList(bestModel.extractParamMap().toSeq());
        int size = paramPairs.size();
        ParamPair[] paramArray = paramPairs.toArray(new ParamPair[size + 3]);

        paramArray[size] = new ParamPair(new Param("", "ldbAiVersion", "LinkoopDB AI version"), version());

        paramArray[size + 1] = new ParamPair(new Param("", "updateTime", "last update time"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        paramArray[size + 2] = new ParamPair(new Param("", "validationMetrics",
            "params and validation jsonString, validation: regression:rmse,binaryClassification:AUC, multiClassification:F1-score"), validationMetricsJson);

        Dataset<MetaDataType> metaDataTypeDataset = MetaDataType.buildDataset(context.getSparkSession(), fit, paramArray);

        return new ModelLogicPlan(metaDataTypeDataset.logicalPlan(), fit);
    }


    /**
     * 用来生成 TrainValidationSplit对象的工厂类
     */
    private class TrainValidationSplitFactory {

        // 选用的模型
        private Estimator estimator;

        // 超参数列表
        private ParamMap[] paramGrids;

        // 评估器
        private Evaluator evaluator;

        // 训练集与验证集的比例
        private double trainRatio;


        TrainValidationSplitFactory(String estimator, Map<String, Object> paramsMap, String evaluator, double trainRatio) throws Exception {
            initialize(estimator, paramsMap, evaluator, trainRatio);
        }

        /**
         * 工厂的初始化方法:
         * step1.根据用户传来的Estimator字符串创建对应的Estimator
         * step2.构建网格搜索参数列表
         * step3.根据用户传来的Evaluator字符串创建对应的Evaluator
         * step4.设置训练集占的比率
         *
         * @throws Exception 如果用户传来的Estimator或者Evaluator不在支持的算法模型中, 将报错
         */
        private void initialize(String estimator, Map<String, Object> paramsMap, String evaluator, double trainRatio) throws Exception {

            String esimatorUpper = estimator.toUpperCase();
            String evaluatorUpper = evaluator.toUpperCase();

            // 此段代码负责step1和step2
            switch (esimatorUpper) {
                case "GBTCLASSIFIER":
                    GBTClassifier gbtClassifier = new GBTClassifier();
                    this.paramGrids = getGBTClassifierParamGrids(gbtClassifier, paramsMap);
                    this.estimator = gbtClassifier;
                    break;
                case "RANDOMFORESTCLASSIFIER":
                    RandomForestClassifier randomForestClassifier = new RandomForestClassifier();
                    this.paramGrids = getRandomForestClassifierParamGrids(randomForestClassifier, paramsMap);
                    this.estimator = randomForestClassifier;
                    break;
                case "DECISIONTREECLASSIFIER":
                    DecisionTreeClassifier decisionTreeClassifier = new DecisionTreeClassifier();
                    this.paramGrids = getDecisionTreeClassifierParamGrids(decisionTreeClassifier, paramsMap);
                    this.estimator = decisionTreeClassifier;
                    break;
                case "LOGISTICREGRESSION":
                    LogisticRegression logisticRegression = new LogisticRegression();
                    this.paramGrids = getLogisticRegressoinParamGrids(logisticRegression, paramsMap);
                    this.estimator = logisticRegression;
                    break;
                case "LINEARREGRESSION":
                    LinearRegression linearRegression = new LinearRegression();
                    this.paramGrids = getLinearRegressionParamGrids(linearRegression, paramsMap);
                    this.estimator = linearRegression;
                    break;
                default:
                    throw new Exception(
                        "unsupported model, now support: GBTClassifier,RandomForestClassifier,DecisionTreeClassifier,"
                            + "LogisticRegression,LinearRegression  . your input is: "
                            + estimator);
            }

            // step3
            switch (evaluatorUpper) {
                case "BINARYCLASSIFICATIONEVALUATOR":
                    this.evaluator = new BinaryClassificationEvaluator();
                    break;
                case "MULTICLASSCLASSIFICATIONEVALUATOR":
                    this.evaluator = new MulticlassClassificationEvaluator();
                    break;
                case "REGRESSIONEVALUATOR":
                    this.evaluator = new org.apache.spark.ml.evaluation.RegressionEvaluator();
                    break;
                default:
                    throw new Exception(
                        "unsupported evaluator , now support: BinaryClassificationEvaluator,"
                            + " MulticlassClassificationEvaluator and RegressionEvaluator. your input is: "
                            + evaluator);
            }

            // step4
            this.trainRatio = trainRatio;
        }

        /**
         * 获取TrainValidationSplit对象
         */
        private org.apache.spark.ml.tuning.TrainValidationSplit getInstance() {
            org.apache.spark.ml.tuning.TrainValidationSplit trainValidationSplit = new org.apache.spark.ml.tuning.TrainValidationSplit();
            trainValidationSplit.setEstimator(this.estimator);
            trainValidationSplit.setEvaluator(this.evaluator);
            trainValidationSplit.setEstimatorParamMaps(this.paramGrids);
            trainValidationSplit.setTrainRatio(this.trainRatio);
            return trainValidationSplit;
        }


        /**
         * 设置洛吉斯特回归的参数列表
         */
        private ParamMap[] getLogisticRegressoinParamGrids(LogisticRegression logisticRegression, Map<String, Object> paramsMap) {

            ParamGridBuilder builder = new ParamGridBuilder();

            for (String key : paramsMap.keySet()) {
                setRegeressionParams(paramsMap, builder, key, logisticRegression.regParam(), logisticRegression.elasticNetParam(), logisticRegression.maxIter(),
                    logisticRegression.tol());
                if ("threshold".equalsIgnoreCase(key)) {
                    setDoubleParam(paramsMap, builder, key, logisticRegression.threshold());
                }
                if ("family".equalsIgnoreCase(key)) {
                    setStringParam(paramsMap, builder, key, logisticRegression.family());
                }
                if ("fitIntercept".equalsIgnoreCase(key)) {
                    builder.addGrid(logisticRegression.fitIntercept());
                }
                if ("standardization".equalsIgnoreCase(key)) {
                    builder.addGrid(logisticRegression.standardization());
                }
            }

            return builder.build();
        }

        private void setRegeressionParams(Map<String, Object> paramsMap, ParamGridBuilder builder, String key, DoubleParam doubleParam,
            DoubleParam doubleParam2, IntParam intParam, DoubleParam tol) {
            if ("regParam".equalsIgnoreCase(key)) {
                setDoubleParam(paramsMap, builder, key, doubleParam);
            }
            if ("elasticNetParam".equalsIgnoreCase(key)) {
                setDoubleParam(paramsMap, builder, key, doubleParam2);
            }
            if ("maxIter".equalsIgnoreCase(key)) {
                setIntParam(paramsMap, builder, key, intParam);
            }
            if ("tol".equalsIgnoreCase(key)) {
                setDoubleParam(paramsMap, builder, key, tol);
            }
        }

        /**
         * 设置决策树的参数列表
         */
        private ParamMap[] getDecisionTreeClassifierParamGrids(DecisionTreeClassifier decisionTreeClassifier, Map<String, Object> paramsMap) {

            return getTreeClassifierParams(paramsMap, decisionTreeClassifier.maxDepth(), decisionTreeClassifier.maxBins(),
                decisionTreeClassifier.minInstancesPerNode(), decisionTreeClassifier.minInfoGain(), decisionTreeClassifier.maxMemoryInMB(),
                decisionTreeClassifier.cacheNodeIds());

        }


        /**
         * 设置随机森林的参数列表
         */
        private ParamMap[] getRandomForestClassifierParamGrids(RandomForestClassifier randomForestClassifier, Map<String, Object> paramsMap) {

            return getTreeClassifierParams(paramsMap, randomForestClassifier.maxDepth(), randomForestClassifier.maxBins(),
                randomForestClassifier.minInstancesPerNode(), randomForestClassifier.minInfoGain(), randomForestClassifier.maxMemoryInMB(),
                randomForestClassifier.cacheNodeIds());

        }

        /**
         * 梯度提升树的参数列表赋值
         */
        private ParamMap[] getGBTClassifierParamGrids(GBTClassifier gbtClassifier, Map<String, Object> paramsMap) {
            return getTreeClassifierParams(paramsMap, gbtClassifier.maxDepth(), gbtClassifier.maxBins(), gbtClassifier.minInstancesPerNode(),
                gbtClassifier.minInfoGain(), gbtClassifier.maxMemoryInMB(), gbtClassifier.cacheNodeIds());

        }

        /**
         * 获取树分类器的参数列表
         */
        private ParamMap[] getTreeClassifierParams(Map<String, Object> paramsMap, IntParam intParam, IntParam intParam2, IntParam intParam3,
            DoubleParam doubleParam, IntParam intParam4, BooleanParam booleanParam) {
            ParamGridBuilder builder = new ParamGridBuilder();

            setTreeClassifierParamGrids(paramsMap, builder, intParam, intParam2, intParam3, doubleParam, intParam4, booleanParam);

            return builder.build();
        }


        /**
         * 设置LinearRegression的超参数候选列表
         */
        private ParamMap[] getLinearRegressionParamGrids(LinearRegression linearRegression, Map<String, Object> paramsMap) {
            ParamGridBuilder builder = new ParamGridBuilder();

            for (String key : paramsMap.keySet()) {
                setRegeressionParams(paramsMap, builder, key, linearRegression.regParam(), linearRegression.elasticNetParam(), linearRegression.maxIter(),
                    linearRegression.tol());
                if ("aggregationDepth".equalsIgnoreCase(key)) {
                    setIntParam(paramsMap, builder, key, linearRegression.aggregationDepth());
                }
                if ("solver".equalsIgnoreCase(key)) {
                    setStringParam(paramsMap, builder, key, linearRegression.solver());
                }
                if ("epsilon".equalsIgnoreCase(key)) {
                    setDoubleParam(paramsMap, builder, key, linearRegression.epsilon());
                }
                if ("loss".equalsIgnoreCase(key)) {
                    setStringParam(paramsMap, builder, key, linearRegression.loss());
                }
                if ("fitIntercept".equalsIgnoreCase(key)) {
                    builder.addGrid(linearRegression.fitIntercept());
                }
                if ("standardization".equalsIgnoreCase(key)) {
                    builder.addGrid(linearRegression.standardization());
                }
            }

            return builder.build();

        }


        /**
         * 树类分类器参数赋值
         */
        private void setTreeClassifierParamGrids(Map<String, Object> paramsMap, ParamGridBuilder builder, IntParam intParamMaxDepth, IntParam intParamMaxBins,
            IntParam intParamMinInstancesPerNode, DoubleParam doubleParamMinInfoGain, IntParam intParamMaxMemoryInMB, BooleanParam cacheNodeIds) {
            for (String key : paramsMap.keySet()) {

                if ("maxDepth".equalsIgnoreCase(key)) {
                    setIntParam(paramsMap, builder, key, intParamMaxDepth);
                }
                if ("maxBins".equalsIgnoreCase(key)) {
                    setIntParam(paramsMap, builder, key, intParamMaxBins);
                }
                if ("minInstancesPerNode".equalsIgnoreCase(key)) {
                    setIntParam(paramsMap, builder, key, intParamMinInstancesPerNode);
                }
                if ("minInfoGain".equalsIgnoreCase(key)) {
                    setDoubleParam(paramsMap, builder, key, doubleParamMinInfoGain);
                }
                if ("maxMemoryInMB".equalsIgnoreCase(key)) {
                    setIntParam(paramsMap, builder, key, intParamMaxMemoryInMB);
                }
                if ("cacheNodeIds".equalsIgnoreCase(key)) {
                    builder.addGrid(cacheNodeIds);
                }

            }
        }

        /**
         * 设置浮点型参数
         */
        private void setDoubleParam(Map<String, Object> paramsMap, ParamGridBuilder builder, String key, DoubleParam doubleParam) {
            List<Double> doubleList = (List<Double>) paramsMap.get(key);
            double[] doubleParams = new double[doubleList.size()];
            for (int i = 0; i < doubleParams.length; i++) {
                doubleParams[i] = doubleList.get(i);
            }
            builder.addGrid(doubleParam, doubleParams);
        }

        /**
         * 设置整型参数
         */
        private void setIntParam(Map<String, Object> paramsMap, ParamGridBuilder builder, String key, IntParam intParam) {
            // Gson转换过来的数字类型都是Double类型 ,直接转Integer会报错
            List<Double> doubleList = (List<Double>) paramsMap.get(key);
            int[] intParams = new int[doubleList.size()];
            for (int i = 0; i < intParams.length; i++) {

                intParams[i] = doubleList.get(i).intValue();
            }
            builder.addGrid(intParam, intParams);
        }

        /**
         * 设置字符串类型的参数
         */
        private void setStringParam(Map<String, Object> paramsMap, ParamGridBuilder builder, String key, Param<String> stringParam) {
            List<String> stringList = (List<String>) paramsMap.get(key);
            Seq<String> stringParams = JavaConverters.asScalaIteratorConverter(stringList.iterator()).asScala().toSeq();
            builder.addGrid(stringParam, stringParams);
        }


    }


}
