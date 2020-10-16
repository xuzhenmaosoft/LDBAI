package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import java.util.Collections;
import java.util.List;
import java.util.Stack;

import com.google.common.collect.Lists;

import com.datapps.linkoopdb.worker.spi.plan.core.LdbDistinct;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbProject;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbUnion;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;


public class CombineUnions extends Rule{

    public static final CombineUnions INSTANCE = new CombineUnions();

    @Override
    public RelNode process(RelNode relNode) {
        return relNode.transform(rel -> {
            if (rel instanceof LdbUnion) {
                return flattenUnion((LdbUnion) rel, false);
            }
            if (rel instanceof LdbDistinct && rel.getChildren().get(0) instanceof LdbUnion) {
                return new LdbDistinct(flattenUnion((LdbUnion) rel, true));
            }
            return rel;
        });
    }

    private LdbUnion flattenUnion(LdbUnion union, Boolean flattenDistinct) {
        Stack<RelNode> stack = new Stack<>();
        stack.push(union);
        List<RelNode> flattened = Lists.newArrayList();

        while (!stack.isEmpty()) {
            RelNode stackTop = stack.pop();
            if (stackTop instanceof LdbDistinct
                && (stackTop.getChildren().get(0) instanceof LdbUnion
                || (stackTop instanceof LdbProject && stackTop.getChildren().get(0) instanceof LdbUnion)) && flattenDistinct) {
                LdbUnion childUnion = (LdbUnion) stackTop.getChildren().get(0);
                List<RelNode> childList = stackTop.getChildren();
                Collections.reverse(childList);
                for (RelNode child : childUnion.getChildren()) {
                    stack.push(child);
                }
            } else if (stackTop instanceof LdbUnion
                || (stackTop instanceof LdbProject && stackTop.getChildren().get(0) instanceof LdbUnion)
                || (stackTop instanceof LdbProject && stackTop.getChildren().get(0) instanceof LdbProject
                    && stackTop.getChildren().get(0).getChildren().get(0) instanceof LdbUnion)) {
                List<RelNode> childList=Lists.newArrayList();
                childList.addAll(stackTop.getChildren());
                Collections.reverse(childList);
                for (RelNode child : childList) {
                    stack.push(child);
                }
            } else {
                flattened.add(stackTop);
            }
        }

        return new LdbUnion(flattened, union.all);
    }
}
