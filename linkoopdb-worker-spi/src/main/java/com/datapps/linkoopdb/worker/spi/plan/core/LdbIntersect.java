package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/8.
 */
public class LdbIntersect extends SetOp {

    public LdbIntersect(List<RelNode> children, boolean all) {
        super(children, SetOp.INTERSECT, all);
    }

    @Override
    public List<RexNode> buildOutput() {
        return getChildren().get(0).getOutput();
    }

    @Override
    public LdbIntersect transformExpression(Transformer transformer) {
        return this;
    }

    @Override
    public LdbIntersect withNewChildren(List<RelNode> children) {
        if (!this.getChildren().equals(children)) {
            this.setChildren(children);
            return new LdbIntersect(this.getChildren(), this.all);
        }
        return this;
    }
}
