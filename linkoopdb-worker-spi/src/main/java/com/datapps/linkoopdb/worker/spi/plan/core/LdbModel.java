package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;
import com.datapps.linkoopdb.worker.spi.plan.func.LdbFunctionCall;

/**
 * @author xingbu
 * @version 1.0 <p> created by　19-1-24 下午4:13
 */
public class LdbModel extends LdbPipelineStage {

    public List<String> parameters;
    public List<RexNode> arguments;
    protected SqlKind createSqlkind;
    private String schemaName;
    private String modelName;

    /**
     * 模型加载的时候用的构造方法
     *
     * @param type 模型的类型
     * @param source 数据源
     * @param createSqlkind 创建时的SQL
     * @param parameters 参数名
     * @param arguments 参数
     */
    public LdbModel(FunctionRelType type, LdbSource source,
        SqlKind createSqlkind, List<String> parameters,
        List<RexNode> arguments, String schemaName, String modelName) {
        super(type, source);
        this.createSqlkind = createSqlkind;
        this.parameters = parameters;
        this.arguments = arguments;
        this.isBuilt = true;
        this.schemaName = schemaName;
        this.modelName = modelName;
    }

    /**
     * 模型训练时用的构造方法
     *
     * @param type 模型的类型
     * @param source 数据源
     * @param createSqlkind 创建时的SQL
     * @param buildFuntion 创建模时需要调用的LdbFunction
     */
    public LdbModel(FunctionRelType type, LdbSource source,
        SqlKind createSqlkind, LdbFunctionCall buildFuntion,
        String schemaName, String modelName) {
        super(type, source);
        this.createSqlkind = createSqlkind;
        this.stageFunction = buildFuntion;
        buildFuntion.invoker = this;
        this.parameters = buildFuntion.parameter;
        this.arguments = buildFuntion.operands;
        this.isBuilt = false;
        this.schemaName = schemaName;
        this.modelName = modelName;
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.copyOf(arguments);
    }

    @Override
    protected String argString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        stringBuilder.append("TYPE(");
        switch (type) {
            case SPARK_MODEL_TRANSFORM:
            case SPARK_MODEL_TRAIN:
                stringBuilder.append("SPARK_ML");
                break;
            case EXTERNAL_MODEL_TRANSFORM:
            case TENSORFLOW_MODEL_TRAIN:
            case TENSORFLOW_MODEL_TRANSFORM:
            case EXTERNAL_MODEL_LOAD:
                stringBuilder.append("PYTHON");
                break;
        }
        stringBuilder.append(") ALGORITHM(");
        stringBuilder.append(createSqlkind.sql
            .replaceAll("_LOAD", "")
            .replaceAll("_TRAIN", "")
            .replaceAll("_TRANSFORMER", "")
            .replaceAll("_PREDICT", "")
        );
        stringBuilder.append(")]");
        return stringBuilder.toString();
    }

    public List<String> getParameters() {
        return parameters;
    }

    public void setParameters(List<String> parameters) {
        this.parameters = parameters;
    }

    public List<RexNode> getArguments() {
        return arguments;
    }

    public void setArguments(
        List<RexNode> arguments) {
        this.arguments = arguments;
    }

    public SqlKind getCreateSqlkind() {
        return createSqlkind;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getModelName() {
        return modelName;
    }

    @Override
    public LdbPipelineStageEntity getStageEntity() {
        return this;
    }

    @Override
    public RexNode getFunctionRexNode() {
        return stageFunction;
    }

    @Override
    public void setFunctionRexNode(RexNode function) {
        stageFunction = function;
    }

}
