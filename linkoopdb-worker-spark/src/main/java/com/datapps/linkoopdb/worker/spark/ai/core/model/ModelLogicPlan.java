/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spark.ai.core.model;

import org.apache.spark.ml.Transformer;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.IsNotNull$;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class ModelLogicPlan extends Filter {

    private LogicalPlan metadataLogicPlan;
    private Transformer model;

    public ModelLogicPlan(Expression condition, LogicalPlan child) {
        super(condition, child);
    }

    public ModelLogicPlan(LogicalPlan metadataLogicPlan, Transformer model) {
        super(IsNotNull$.MODULE$.apply(Literal$.MODULE$.apply("a")), metadataLogicPlan);
        this.metadataLogicPlan = metadataLogicPlan;
        this.model = model;
    }

    public LogicalPlan getMetadataLogicPlan() {
        return metadataLogicPlan;
    }

    public void setMetadataLogicPlan(LogicalPlan metadataLogicPlan) {
        this.metadataLogicPlan = metadataLogicPlan;
    }

    public Transformer getModel() {
        return model;
    }

    public void setModel(Transformer model) {
        this.model = model;
    }
}
