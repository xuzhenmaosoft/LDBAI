package com.datapps.linkoopdb.worker.spark.ai.core;

import java.util.Map;

import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;

/**
 * @author xingbu
 * @version 1.0
 *
 *          created by　19-3-21 下午3:40
 */
public abstract class FunctionContext {

    /**
     * 包含所有表函数的集合，key为算法名，value为对应的LdbFunction的实现类 主要是为了pipeline使用
     */
    private Map ldbFunctionMap;

    private SqlKind functionKind;

    private String[] parameters;

    private Object[] arguments;

    public FunctionContext() {
    }

    public FunctionContext(Map ldbFunctionMap, SqlKind functionName, String[] parameters,
        Object[] arguments) {
        this.ldbFunctionMap = ldbFunctionMap;
        this.functionKind = functionName;
        this.parameters = parameters;
        this.arguments = arguments;
    }

    public Map getLdbFunctionMap() {
        return ldbFunctionMap;
    }

    public void setLdbFunctionMap(Map ldbFunctionMap) {
        this.ldbFunctionMap = ldbFunctionMap;
    }

    public Object[] getArguments() {
        return arguments;
    }

    public void setArguments(Object[] arguments) {
        this.arguments = arguments;
    }

    public String getFunctionName() {
        return functionKind.sql;
    }

    public void setFunctionName(String functionName) {
        this.functionKind = SqlKind.convertLdbFunctionByName(functionName);
    }

    public SqlKind getFunctionSqlKind() {
        return functionKind;
    }

    public void setFunctionKind(SqlKind functionKind) {
        this.functionKind = functionKind;
    }

    public String[] getParameters() {
        return parameters;
    }

    public void setParameters(String[] parameters) {
        this.parameters = parameters;
    }

}
