package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import java.util.List;

import com.datapps.linkoopdb.worker.spi.plan.core.LdbAggregate;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbDistinct;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;

public class ReplaceDistinctWithAggregate extends Rule {

    @Override
    public RelNode process(RelNode relNode) {
        return relNode.transform((rel) -> {
            if (rel instanceof LdbDistinct) {
                //TODO list转换
                LdbAggregate ldbAggregate = new LdbAggregate(((LdbDistinct) rel).getOutput(),
                    (List<NamedRexNode>) (List) ((LdbDistinct) rel).child.getOutput(),
                    ((LdbDistinct) rel).child);
                return ldbAggregate;
            }
            return rel;
        });
    }
}
