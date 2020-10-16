/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import com.datapps.linkoopdb.jdbc.LdbSqlNameManager.LdbSqlName;
import com.datapps.linkoopdb.jdbc.SchemaObject;
import com.datapps.linkoopdb.jdbc.SessionInterface;
import com.datapps.linkoopdb.jdbc.Tokens;
import com.datapps.linkoopdb.jdbc.lib.OrderedHashSet;
import com.datapps.linkoopdb.jdbc.rights.Grantee;

/**
 * Created by gloway on 2018/6/4.
 */
public class ResourceGroup implements SchemaObject, Cloneable {

    public boolean isDefault = false;
    protected LdbSqlName groupName;
    private double percentage;

    private double globalMinPercentage;

    private double globalMaxPercentage;

    private boolean dynamicAlloction = false;

    private long memory = 0;
    private int cpuCores = 0;
    private int gpuCores = 0;

    // advanced options
    // 从配置中读取
    private long configManagerMemory;
    private int configNumExecutors;
    private int configExecutorCores;
    private long configExecutorMemory;
    private int configGpuCores;

    private String hadoopConfDir;
    private String queue;
    private String hadoopUser;
    private String keytab;
    // 经过计算后
    private long managerMemory;
    // 默认为1
    private int managerCores = 1;
    private int numExecutors;
    private int minExecutors;
    private int maxExecutors;
    private int executorCores;
    private long executorMemory;
    //default:0 <-> BATCH, 1 <-> STREAM
    private int executorType = 0;

    public static int BATCH_EXECUTOR_TYPE = 0;
    public static int STREAM_EXECUTOR_TYPE = 1;

    public ResourceGroup(LdbSqlName name) {
        this.groupName = name;
    }

    @Override
    public int getType() {
        return RESOURCE_GROUP;
    }

    @Override
    public LdbSqlName getName() {
        return groupName;
    }

    @Override
    public LdbSqlName getSchemaName() {
        return null;
    }

    @Override
    public LdbSqlName getCatalogName() {
        return null;
    }

    @Override
    public Grantee getOwner() {
        return null;
    }

    @Override
    public OrderedHashSet getReferences() {
        return null;
    }

    @Override
    public OrderedHashSet getComponents() {
        return null;
    }

    @Override
    public void compile(SessionInterface session, SchemaObject parentObject) {

    }

    @Override
    public String getSQL() {
        StringBuffer sb = new StringBuffer();
        sb.append(Tokens.T_CREATE).append(' ').append(Tokens.T_RESOURCE).append(' ').append(Tokens.T_GROUP)
            .append(' ').append(groupName.getStatementName()).append(' ');
        if (percentage != 0) {
            sb.append(Tokens.T_GLOBAL).append(' ').append(Tokens.T_PERCENT).append(' ').append(percentage).append(' ');
        }
        if (globalMinPercentage != 0) {
            sb.append(Tokens.T_GLOBAL).append(' ').append(Tokens.T_MIN).append(' ').append(Tokens.T_PERCENT).append(' ').append(globalMinPercentage)
                .append(' ');
        }
        if (globalMaxPercentage != 0) {
            sb.append(Tokens.T_GLOBAL).append(' ').append(Tokens.T_MAX).append(' ').append(Tokens.T_PERCENT).append(' ').append(globalMaxPercentage)
                .append(' ');
        }
        if (memory != 0 && cpuCores != 0) {
            sb.append(Tokens.T_MEMORY).append(' ').append(memory).append(' ').append(Tokens.T_CPU).append(' ').append(cpuCores).append(' ');
        }
        sb.append(Tokens.T_MANAGER).append(' ').append(Tokens.T_MEMORY).append(' ').append(managerMemory).append(' ')
            .append(Tokens.T_MANAGER).append(' ').append(Tokens.T_CPU).append(' ').append(managerCores).append(' ')
            .append(Tokens.T_NUMBER).append(' ').append(Tokens.T_EXECUTOR).append(' ').append(numExecutors).append(' ')
            .append(Tokens.T_EXECUTOR).append(' ').append(Tokens.T_MEMORY).append(' ').append(executorMemory).append(' ')
            .append(Tokens.T_EXECUTOR).append(' ').append(Tokens.T_CPU).append(' ').append(executorCores).append(' ');
        if (executorType == STREAM_EXECUTOR_TYPE) {
            sb.append(Tokens.T_EXECUTOR).append(' ').append(Tokens.T_TYPE).append(' ').append(Tokens.T_STREAM).append(' ');
        }
        sb.append(' ').append(Tokens.T_DYNAMIC).append(' ').append(Tokens.T_ALLOCATE).append(' ').append(Boolean.toString(dynamicAlloction));
        return sb.toString();
    }

    @Override
    public long getChangeTimestamp() {
        return 0;
    }

    public double getPercentage() {
        return percentage;
    }

    public void setPercentage(double percentage) {
        this.percentage = percentage;
    }

    public double getGlobalMinPercentage() {
        return globalMinPercentage;
    }

    public void setGlobalMinPercentage(double globalMinPercentage) {
        this.globalMinPercentage = globalMinPercentage;
    }

    public double getGlobalMaxPercentage() {
        return globalMaxPercentage;
    }

    public void setGlobalMaxPercentage(double globalMaxPercentage) {
        this.globalMaxPercentage = globalMaxPercentage;
    }

    public boolean isDynamicAlloction() {
        return dynamicAlloction;
    }

    public void setDynamicAlloction(boolean dynamicAlloction) {
        this.dynamicAlloction = dynamicAlloction;
    }

    public long getMemory() {
        return memory;
    }

    public void setMemory(long memory) {
        this.memory = memory;
    }

    public int getCpuCores() {
        return cpuCores;
    }

    public void setCpuCores(int cpuCores) {
        this.cpuCores = cpuCores;
    }

    public long getManagerMemory() {
        return managerMemory;
    }

    public void setManagerMemory(long managerMemory) {
        this.managerMemory = managerMemory;
    }

    public int getManagerCores() {
        return managerCores;
    }

    public void setManagerCores(int managerCores) {
        this.managerCores = managerCores;
    }

    public int getConfigGpuCores() {
        return configGpuCores;
    }

    public void setConfigGpuCores(int configGpuCores) {
        this.configGpuCores = configGpuCores;
    }

    public int getNumExecutors() {
        return numExecutors;
    }

    public void setNumExecutors(int numExecutors) {
        this.numExecutors = numExecutors;
    }

    public int getMinExecutors() {
        return minExecutors;
    }

    public void setMinExecutors(int minExecutors) {
        this.minExecutors = minExecutors;
    }

    public int getMaxExecutors() {
        return maxExecutors;
    }

    public void setMaxExecutors(int maxExecutors) {
        this.maxExecutors = maxExecutors;
    }

    public int getExecutorCores() {
        return executorCores;
    }

    public void setExecutorCores(int executorCores) {
        this.executorCores = executorCores;
    }

    public long getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(long executorMemory) {
        this.executorMemory = executorMemory;
    }

    public ResourceGroup clone() throws CloneNotSupportedException {
        return (ResourceGroup) super.clone();
    }

    public int getGpuCores() {
        return gpuCores;
    }

    public void setGpuCores(int gpuCores) {
        this.gpuCores = gpuCores;
    }

    public long getConfigManagerMemory() {
        return configManagerMemory;
    }

    public void setConfigManagerMemory(long configManagerMemory) {
        this.configManagerMemory = configManagerMemory;
    }

    public int getConfigNumExecutors() {
        return configNumExecutors;
    }

    public void setConfigNumExecutors(int configNumExecutors) {
        this.configNumExecutors = configNumExecutors;
    }

    public int getConfigExecutorCores() {
        return configExecutorCores;
    }

    public void setConfigExecutorCores(int configExecutorCores) {
        this.configExecutorCores = configExecutorCores;
    }

    public long getConfigExecutorMemory() {
        return configExecutorMemory;
    }

    public void setConfigExecutorMemory(long configExecutorMemory) {
        this.configExecutorMemory = configExecutorMemory;
    }

    public String getHadoopConfDir() {
        return hadoopConfDir;
    }

    public void setHadoopConfDir(String hadoopConfDir) {
        this.hadoopConfDir = hadoopConfDir;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getHadoopUser() {
        return hadoopUser;
    }

    public void setHadoopUser(String hadoopUser) {
        this.hadoopUser = hadoopUser;
    }

    public String getKeytab() {
        return keytab;
    }

    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    public int getExecutorType() {
        return executorType;
    }

    public void setExecutorType(int executorType) {
        this.executorType = executorType;
    }

    public boolean isBatchResourceGroup() {
        return this.executorType == ResourceGroup.BATCH_EXECUTOR_TYPE;
    }

    public boolean isStreamResourceGroup() {
        return this.executorType == ResourceGroup.STREAM_EXECUTOR_TYPE;
    }
}
