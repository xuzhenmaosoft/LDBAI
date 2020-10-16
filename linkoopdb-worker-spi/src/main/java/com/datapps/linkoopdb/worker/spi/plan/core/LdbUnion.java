package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;
import java.util.stream.Collectors;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;
import com.datapps.linkoopdb.worker.spi.plan.util.Utilities;
import org.apache.commons.collections.CollectionUtils;

/**
 * Created by gloway on 2019/1/8.
 */
public class LdbUnion extends SetOp {

    public LdbUnion(List<RelNode> children, boolean all) {
        super(children, SetOp.UNION, all);
    }

    @Override
    public List<RexNode> buildOutput() {
        List<List<RexNode>> childOutput = getChildren().stream().map(RelNode::getOutput).collect(Collectors.toList());
        List<List<RexNode>> transposeList = Utilities.transpose(childOutput);

        transposeList.forEach(list -> {
            if (CollectionUtils.exists(list, item -> ((RexNode) item).nullable)) {
                list.get(0).nullable = true;
            }
        });
        return childOutput.get(0);
    }

    @Override
    public LdbUnion transformExpression(Transformer transformer) {
        return this;
    }

    @Override
    public LdbUnion withNewChildren(List<RelNode> children) {
        if (!this.getChildren().equals(children)) {
            return new LdbUnion(children, this.all);
        }
        return this;
    }
}
