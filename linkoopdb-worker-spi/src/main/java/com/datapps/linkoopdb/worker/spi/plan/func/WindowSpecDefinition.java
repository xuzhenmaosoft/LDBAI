package com.datapps.linkoopdb.worker.spi.plan.func;

import java.util.ArrayList;
import java.util.List;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SortOrderRexNode;

/**
 * Created by gloway on 2019/1/29.
 */
public class WindowSpecDefinition extends RexNode {

    public List<RexNode> partitionSpec;
    public List<SortOrderRexNode> orderSpec;
    public SqlWindowFrame frameSpecification;

    public WindowSpecDefinition(List<RexNode> partitionSpec, List<SortOrderRexNode> orderSpec, SqlWindowFrame frameSpecification) {
        this.partitionSpec = partitionSpec;
        this.orderSpec = orderSpec;
        this.frameSpecification = frameSpecification;
    }

    @Override
    public List<RexNode> getChildren() {
        ArrayList<RexNode> output = new ArrayList<>();
        output.addAll(partitionSpec);
        output.addAll(orderSpec);
        output.add(frameSpecification);
        return output;
    }

    @Override
    public WindowSpecDefinition withNewChildren(List<RexNode> children) {
        return this;
    }

    @Override
    public String simpleString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("windowspecdefinition(");
        for (RexNode part : partitionSpec) {
            stringBuilder.append(part.simpleString() + ", ");
        }

        for (SortOrderRexNode sortOrder : orderSpec) {
            stringBuilder.append(sortOrder.simpleString() + ", ");
        }
        stringBuilder.append(frameSpecification.simpleString());
        stringBuilder.append(")");
        return stringBuilder.toString();
    }
}
