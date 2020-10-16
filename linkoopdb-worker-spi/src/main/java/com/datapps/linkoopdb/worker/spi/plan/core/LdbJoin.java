package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.datapps.linkoopdb.worker.spi.plan.expression.AttributeRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;
import com.datapps.linkoopdb.worker.spi.plan.util.FlatLists;

/**
 * Created by gloway on 2019/1/8.
 */
public class LdbJoin extends BiRel {

    public JoinRelType joinType;
    public RexNode condition;
    private AttributeRexNode existenceJoinAttr;


    public LdbJoin(RelNode left, RelNode right, JoinRelType joinType, RexNode condition) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.condition = condition;
        this.nodeName = "JOIN " + joinType;
    }

    @Override
    public List<RexNode> buildOutput() {
        switch (joinType) {
            case LEFT_SEMI:
                return left.getOutput();
            case LEFT_OUTER:
            case LEFT_ANTI:
            case RIGHT_OUTER:
            case FULL_OUTER:
            case INNER:
            case CROSS:
            default:
                return FlatLists.of(left.getOutput(), right.getOutput());
        }
    }

    @Override
    public LdbJoin withNewChildren(List<RelNode> children) {
        if (!left.fastEquals(children.get(0)) || !right.fastEquals(children.get(0))) {
            left = children.get(0);
            right = children.get(1);
            return new LdbJoin(this.left, this.right, this.joinType, this.condition);
        }
        return this;
    }

    @Override
    protected String argString() {
        if (condition != null) {
            return condition.simpleString();
        } else {
            return "";
        }
    }

    @Override
    public LdbJoin transformExpression(Transformer transformer) {
        RexNode afterRule = condition.transformDown(transformer);
        if (!condition.equals(afterRule)) {
            this.condition = afterRule;
            return new LdbJoin(this.left, this.right, this.joinType, this.condition);
        }
        return this;
    }

    public AttributeRexNode getExistenceJoinAttr() {
        return existenceJoinAttr;
    }

    public void setExistenceJoinAttr(AttributeRexNode existenceJoinAttr) {
        this.existenceJoinAttr = existenceJoinAttr;
    }

}
