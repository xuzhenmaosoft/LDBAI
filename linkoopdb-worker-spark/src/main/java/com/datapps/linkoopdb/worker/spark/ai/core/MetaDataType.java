/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spark.ai.core;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.datapps.linkoopdb.worker.spi.plan.core.LdbPipeline;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbPipelineStage;
import com.datapps.linkoopdb.worker.spi.plan.expression.AliasRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.AttributeRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexCall;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamPair;
import org.apache.spark.ml.param.Params;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.collection.JavaConversions;

/**
 * modified by xingbu
 * <p>
 * 2019-02-28 16:05:33
 * <p>
 * 该类作为模型训练后、以及图在数据库中存储的元数据的数据类型
 */
public class MetaDataType implements Serializable {

    public static final Encoder<MetaDataType> META_DATA_TYPE_ENCODER = Encoders.bean(MetaDataType.class);
    private String name;
    private String value;
    private String doc;


    public MetaDataType(ParamPair pair) {
        setName(pair.param().name());
        setDoc(pair.param().doc());
        Object value = pair.value();
        if (value instanceof Object[]) {
            String values = Arrays.toString((Object[]) value);
            setValue(values);
        } else if (value instanceof int[]) {
            String values = Arrays.toString((int[]) value);
            setValue(values);
        } else if (value instanceof double[]) {
            String values = Arrays.toString((double[]) value);
            setValue(values);
        } else {
            setValue(value.toString());
        }
    }

    public static Dataset<MetaDataType> buildDataset(SparkSession sparkSession, Params model, ParamPair... extraMetas) {
        List<MetaDataType> metaDataTypes = new ArrayList<>(16);
        Consumer<ParamPair> consumer = (ParamPair x) -> {
            Object value = x.value();
            // 判断参数是否为应当存储的类型，不存储无意义的元数据(如:Estimator对象的toString方法生成的无意义数据)
            if (shouldStore(value)) {
                MetaDataType metaData = new MetaDataType(x);
                if (!metaDataTypes.contains(metaData)) {
                    metaDataTypes.add(metaData);
                }
            }
        };
        if (model != null) {
            JavaConversions.seqAsJavaList(model.extractParamMap().toSeq())
                .stream().forEach(consumer);
        }
        if (extraMetas != null) {
            Arrays.asList(extraMetas).forEach(consumer);

        }
        // 如果是模型的话,还需要考虑到训练该模型的Estimator的参数是否已经被存到元数据中
        if (model instanceof Model) {
            Model fittedModel = (Model) model;
            Estimator parent = fittedModel.parent();
            if (parent != null) {
                JavaConversions.seqAsJavaList(parent.extractParamMap().toSeq())
                    .stream().forEach(consumer);
            }
        }
        return sparkSession.createDataset(metaDataTypes, META_DATA_TYPE_ENCODER);
    }

    public static Dataset<MetaDataType> buildPipelineDataset(SparkSession sparkSession, LdbPipeline pipeline, String pipelineVersion) {
        List<MetaDataType> metaDataTypes = new ArrayList<>(16);
        metaDataTypes.add(new MetaDataType(MetaDataType.buidParamPair("ldbAiVersion", "LinkoopDB pipeline version", pipelineVersion)));
        metaDataTypes.add((new MetaDataType(MetaDataType.buidParamPair("updateTime", "last update time",
            new SimpleDateFormat("yyyy-MM-dd " + "HH:mm:ss").format(new Date())))));
        List<LdbPipelineStage> pipelineStages = pipeline.getStages();
        for (int i = 0; i < pipelineStages.size(); i++) {
            LdbPipelineStage stage = pipelineStages.get(i);
            List<String> inputCols = stage.getInputCols().stream().map(x -> ((AttributeRexNode) x).name).collect(Collectors.toList());
            List<String> ouputCols = stage.getOutputCols().stream().map(x -> ((AttributeRexNode) x).name).collect(Collectors.toList());
            RexNode functionRexNode = stage.getFunctionRexNode();
            String functionName;
            if (functionRexNode instanceof AliasRexNode) {
                RexCall rexCall = (RexCall) ((AliasRexNode) functionRexNode).child;
                functionName = rexCall.sqlKind.sql;
            } else {
                functionName = ((RexCall) functionRexNode).sqlKind.sql;
            }
            String stepName = (i + 1) + "_" + stage.getStepName();
            MetaDataType metaData = new MetaDataType(buidParamPair(stepName, "inputCols:" + inputCols + " , outputCols:" + ouputCols, functionName));
            metaDataTypes.add(metaData);
        }
        return sparkSession.createDataset(metaDataTypes, META_DATA_TYPE_ENCODER);
    }


    public static ParamPair buidParamPair(String name, String doc, Object value) {
        return new ParamPair(new Param("", name, doc), value);
    }

    /**
     * 判断该参数是否应该进行存储
     */
    private static boolean shouldStore(Object value) {
        return value instanceof String || value instanceof Number || value instanceof String[] || value instanceof Boolean || value instanceof int[]
            || value instanceof double[];
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDoc() {
        return doc;
    }

    public void setDoc(String doc) {
        this.doc = doc;
    }

    /**
     * 为防止重复写入相同的元数据进行比较,判断依据为参数名是否存在于表中
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetaDataType)) {
            return false;
        }
        MetaDataType that = (MetaDataType) o;
        return Objects.equals(name, that.name);
    }

}
