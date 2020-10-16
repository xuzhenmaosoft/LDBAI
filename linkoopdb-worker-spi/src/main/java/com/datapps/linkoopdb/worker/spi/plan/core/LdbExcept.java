package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/10.
 */
public class LdbExcept extends SetOp {

    public LdbExcept(List<RelNode> children, boolean all) {
        super(children, SetOp.EXCEPT, all);
    }

    @Override
    public List<RexNode> buildOutput() {
        return getChildren().get(0).getOutput();
    }

    @Override
    public LdbExcept transformExpression(Transformer transformer) {
        return this;
    }

    @Override
    public LdbExcept withNewChildren(List<RelNode> children) {
        if (!this.getChildren().equals(children)) {
            this.setChildren(children);
            return new LdbExcept(this.getChildren(), this.all);
        }
        return this;
    }
}
