package com.datapps.linkoopdb.worker.spark.ai.core;

import com.datapps.linkoopdb.worker.spark.ai.core.model.GraphLogicPlan;
import com.datapps.linkoopdb.worker.spi.LdbTableFunction;
import org.apache.spark.ml.param.ParamPair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

public interface GraphTableFunction extends LdbTableFunction, MetaDataStore {

    @Override
    default Object invoke(Object context) throws Exception {
        SparkFunctionContext ctx = (SparkFunctionContext) context;
        switch (ctx.getFunctionName()) {
            case "GRAPH_BUILD":
                return build(ctx);
            case "GRAPH_CONNECTED_COMPONENTS":
                return connectedComponents(ctx);
            case "GRAPH_TRIANGLE_COUNT":
                return triangleCount(ctx);
            case "GRAPH_STRONGLY_CONNECTED_COMPONENTS":
                return stronglyConnectedComponents(ctx);
            case "GRAPH_LABEL_PROPAGATION":
                return labelPropagation(ctx);
            case "GRAPH_N_DEGREE":
                return nDegree(ctx);
            case "GRAPH_N_DEGREE_ALL":
                return nDegreeAll(ctx);
            case "GRAPH_BFS":
                return BFS(ctx);
            case "GRAPH_SHORTEST_PATHS":
                return shortestPath(ctx);
            case "GRAPH_PAGE_RANK":
                return pageRank(ctx);
            case "GRAPH_PAGE_RANK_PREGEL":
                return pageRankPregel(ctx);
            default:
                return null;
        }
    }

    /**
     * 图加载
     *
     * @param meta 表元数据
     * @param vertices 图的顶点表集合
     * @param edges 图的边表的集合
     */
    GraphLogicPlan load(LogicalPlan meta, Dataset<Row> vertices, Dataset<Row> edges);

    /**
     * 创建图
     */
    GraphLogicPlan build(SparkFunctionContext context) throws Exception;

    /**
     * 计算联通子图
     */
    LogicalPlan connectedComponents(SparkFunctionContext context);

    /**
     * 三角形计数
     */
    LogicalPlan triangleCount(SparkFunctionContext context);

    /**
     * 强连通分量
     */
    LogicalPlan stronglyConnectedComponents(SparkFunctionContext context);

    /**
     * 社区发现
     */
    LogicalPlan labelPropagation(SparkFunctionContext context);

    /**
     * n度关系
     */
    LogicalPlan nDegree(SparkFunctionContext context);

    /**
     * n度关系(全量)
     */
    LogicalPlan nDegreeAll(SparkFunctionContext context);

    /**
     * 广度优先搜索
     */
    LogicalPlan BFS(SparkFunctionContext context);

    /**
     * 最短路径
     */
    LogicalPlan shortestPath(SparkFunctionContext context);

    /**
     * 网页排名
     */
    GraphLogicPlan pageRank(SparkFunctionContext context);

    /**
     * 网页排名 (pregel)
     */
    GraphLogicPlan pageRankPregel(SparkFunctionContext context);


    Dataset<MetaDataType> buildMetaData(SparkFunctionContext context,ParamPair[] paramPairs);
}
