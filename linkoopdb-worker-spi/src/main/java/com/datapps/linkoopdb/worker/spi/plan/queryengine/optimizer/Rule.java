package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;


public abstract class Rule {

    public String ruleName = getClass().getName();

    public abstract RelNode process(RelNode relNode);
}
