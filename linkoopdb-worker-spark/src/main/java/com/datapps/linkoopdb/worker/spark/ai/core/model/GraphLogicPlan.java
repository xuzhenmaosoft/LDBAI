/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spark.ai.core.model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.graphframes.GraphFrame;

public class GraphLogicPlan extends ModelLogicPlan {

    private GraphFrame graphFrame;

    public GraphLogicPlan(Expression condition, LogicalPlan child) {
        super(condition, child);
    }

    /**
     * Constructor for graph computing
     */
    public GraphLogicPlan(LogicalPlan metadataLogicPlan, Dataset<Row> vertices, Dataset<Row> edges) {
        super(metadataLogicPlan, null);
        graphFrame = GraphFrame.apply(vertices, edges);
    }

    /**
     * Constructor for graph building
     */
    public GraphLogicPlan(LogicalPlan metadataLogicPlan) {
        super(metadataLogicPlan, null);
    }

    public Dataset getEdges() {
        return graphFrame.edges();
    }

    public Dataset getVertices() {
        return graphFrame.vertices();
    }

    public GraphFrame getGraphFrame() {
        return graphFrame;
    }
}
