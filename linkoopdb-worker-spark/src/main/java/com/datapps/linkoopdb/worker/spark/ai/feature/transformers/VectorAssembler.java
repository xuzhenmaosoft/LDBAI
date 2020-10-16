/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.feature.transformers;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLFeatureBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.util.DatasetUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class VectorAssembler extends SparkMLFeatureBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"vector_assembler", "vector_assembler_transformer"};
    }

    @Override
    public void setFittingParams(Object[] arguments, String[] parameters, PipelineStage pipelineStage) {
        for (int i = 0; i < parameters.length; i++) {
            if (arguments[i] != null && pipelineStage.hasParam(parameters[i])) {
                Object argument = arguments[i];
                if (argument instanceof Object[]) {
                    int length = ((Object[]) argument).length;
                    String[] arra = new String[length];
                    for (int j = 0; j < length; j++) {
                        arra[j] = ((Object[]) argument)[j].toString();
                    }
                    pipelineStage.set(parameters[i], arra);
                    continue;
                }
                pipelineStage.set(parameters[i], arguments[i]);
            }
        }
    }

    @Override
    public Dataset processRows(Dataset rows) {
        StructType schema = rows.schema();
        for (int i = 0; i < rows.schema().size(); i++) {
            StructField structField = schema.apply(i);
            DataType dataType = structField.dataType();
            if (dataType instanceof ArrayType && ((ArrayType) dataType).elementType() instanceof DoubleType) {
                rows = rows.withColumn(structField.name(), DatasetUtils.columnToVector(rows, structField.name()));
            }
        }
        return rows;
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.feature.VectorAssembler()
            .setOutputCol("FEATURES");
    }

    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            org.apache.spark.ml.feature.VectorAssembler.load(path));
    }
}
