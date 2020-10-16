/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spark.ai.core;

import java.util.List;
import java.util.Map;

import com.datapps.linkoopdb.pallas.dist.LDBPallasDistFileSystem;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * modified by xingbu 2019-03-07 20:38:00
 */
public class SparkFunctionContext extends FunctionContext {

    private SparkSession sparkSession;

    private List<String> inputCols;
    private List<String> outputCols;

    /**
     * 记录模型将要保存的路径
     */
    private String targetPath;

    private StorageEngineType storageEngineType;

    public enum StorageEngineType {
        LOCAL, HDFS, PALLAS
    }

    public SparkFunctionContext() {
    }

    public SparkFunctionContext(Map ldbFunctionMap, SqlKind functionName, String[] parameters,
        Object[] arguments, SparkSession sparkSession) {
        super(ldbFunctionMap, functionName, parameters, arguments);
        this.sparkSession = sparkSession;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public List<String> getInputCols() {
        return inputCols;
    }

    public void setInputCols(List<String> inputCols) {
        this.inputCols = inputCols;
    }

    public List<String> getOutputCols() {
        return outputCols;
    }

    public void setOutputCols(List<String> outputCols) {
        this.outputCols = outputCols;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        if (targetPath.startsWith(LDBPallasDistFileSystem.SCHEME)) {
            storageEngineType = StorageEngineType.PALLAS;
        } else if (targetPath.startsWith(HdfsConstants.HDFS_URI_SCHEME)) {
            storageEngineType = StorageEngineType.HDFS;
        } else {
            storageEngineType = StorageEngineType.LOCAL;
        }
        this.targetPath = targetPath;
    }

    public StorageEngineType getStorageEngineType() {
        return storageEngineType;
    }

    public Dataset<Row> getTrainset() {
        LogicalPlan logicalPlan = (LogicalPlan) getArguments()[0];
        return Dataset.ofRows(sparkSession, logicalPlan);
    }

}
