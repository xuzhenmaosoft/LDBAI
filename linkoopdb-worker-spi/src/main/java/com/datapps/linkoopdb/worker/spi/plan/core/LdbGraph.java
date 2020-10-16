package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;
import com.datapps.linkoopdb.worker.spi.plan.func.LdbFunctionCall;

/**
 * @author xingbu
 * @version 1.0
 *
 * created by　19-3-25 下午4:28
 */
public class LdbGraph extends LdbFunctionRel {


    public List<String> parameters;
    public List<RexNode> arguments;

    // 目前只支持有向图
    public static final int DIRECT = 0;
    // 顶点表
    private RelNode verticesOld;
    // 边表
    private RelNode edgesOld;
    private RelNode verticesNew;
    private RelNode edgesNew;
    // 图的类型
    private int graphType;

    private String verticesTable;
    private String edgesTable;

    private SqlKind createSqlkind;


    public LdbGraph(FunctionRelType type, LdbSource source,LdbFunctionCall ldbFunctionCall, RelNode verticesOld, RelNode edgesOld, RelNode verticesNew,
         RelNode edgesNew, int graphType,
        String verticesTable,
        String edgesTable) {
        super(type, source);
        this.arguments = ldbFunctionCall.operands;
        this.parameters = ldbFunctionCall.parameter;
        this.verticesOld = verticesOld;
        this.edgesOld = edgesOld;
        this.verticesNew = verticesNew;
        this.edgesNew = edgesNew;
        this.graphType = graphType;
        this.verticesTable = verticesTable;
        this.edgesTable = edgesTable;
        this.createSqlkind = ldbFunctionCall.sqlKind;
        if (type == FunctionRelType.GRAPH_SIMPLE_BUILD) {
            this.isBuilt = false;
        } else {
            this.isBuilt = true;
        }
    }

    /**
     * Constructor for building by function
     */
    public LdbGraph(FunctionRelType type, LdbSource source, RelNode verticesOld, RelNode edgesOld, RelNode verticesNew, RelNode edgesNew, int graphType,
        String verticesTable,
        String edgesTable) {
        super(type, source);
        this.createSqlkind = SqlKind.GRAPH_BUILD;
        this.verticesOld = verticesOld;
        this.edgesOld = edgesOld;
        this.verticesNew = verticesNew;
        this.edgesNew = edgesNew;
        this.graphType = graphType;
        this.verticesTable = verticesTable;
        this.edgesTable = edgesTable;
        if (type == FunctionRelType.GRAPH_SIMPLE_BUILD) {
            this.isBuilt = false;
        } else {
            this.isBuilt = true;
        }
    }

    /**
     * Constructor for simple building
     */
    public LdbGraph(FunctionRelType type, LdbSource source, RelNode verticesOld, RelNode edgesOld, int graphType,
        String verticesTable,
        String edgesTable) {
        super(type, source);
        this.createSqlkind = SqlKind.GRAPH_BUILD;
        this.verticesOld = verticesOld;
        this.edgesOld = edgesOld;
        this.graphType = graphType;
        this.verticesTable = verticesTable;
        this.edgesTable = edgesTable;
        if (type == FunctionRelType.GRAPH_SIMPLE_BUILD) {
            this.isBuilt = false;
        } else {
            this.isBuilt = true;
        }
    }


    @Override
    public List<RelNode> getInnerChildren() {
        List<RelNode> tables = new ArrayList<>();
        tables.add(verticesOld);
        tables.add(edgesOld);
        return tables;
    }

    @Override
    protected String argString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        stringBuilder.append("TYPE(");
        switch (type) {
            case GRAPH_BUILD:
            case GRAPH_COMPUTE:
                stringBuilder.append("GRAPH");
                break;
        }
        stringBuilder.append(") GRAPH_TYPE(");
        switch (graphType) {
            case DIRECT:
                stringBuilder.append("DIRECT");
                break;
        }
        stringBuilder.append(") VERTICES(" + verticesTable + ")");
        stringBuilder.append(" EDGES(" + edgesTable + ")]");
        return stringBuilder.toString();
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of();
    }

    public RelNode getVerticesOld() {
        return verticesOld;
    }

    public RelNode getEdgesOld() {
        return edgesOld;
    }

    public RelNode getVerticesNew() {
        return verticesNew;
    }

    public RelNode getEdgesNew() {
        return edgesNew;
    }

    public int getGraphType() {
        return graphType;
    }

    public String getVerticesTable() {
        return verticesTable;
    }

    public String getEdgesTable() {
        return edgesTable;
    }

    public SqlKind getCreateSqlkind() {
        return createSqlkind;
    }

    public void setVerticesOld(RelNode verticesOld) {
        this.verticesOld = verticesOld;
    }

    public void setEdgesOld(RelNode edgesOld) {
        this.edgesOld = edgesOld;
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

    public void setArguments(List<RexNode> arguments) {
        this.arguments = arguments;
    }
}
