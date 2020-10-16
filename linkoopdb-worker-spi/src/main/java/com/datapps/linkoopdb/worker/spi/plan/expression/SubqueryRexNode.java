package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.Lists;

import com.datapps.linkoopdb.worker.spi.plan.ExprId;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;

/**
 * Created by gloway on 2019/1/9.
 */
public class SubqueryRexNode<T extends RelNode> extends RexNode {

    public ExprId exprId;
    public T plan;
    public boolean isScalarSubquery;
    protected List<RexNode> children;


    public SubqueryRexNode(T plan, List<RexNode> children) {
        this.plan = plan;
        this.children = children;
        this.exprId = NamedRexNode.newExprId();
    }

    public SubqueryRexNode(T plan, List<RexNode> children, boolean isScalarSubquery) {
        this.plan = plan;
        this.children = children;
        this.exprId = NamedRexNode.newExprId();
        this.isScalarSubquery = isScalarSubquery;
    }

    // 只有对于关联标量子查询才有 children
    @Override
    public List<RexNode> getChildren() {
        return children;
    }

    @Override
    public String simpleString() {

        String s = mkString(children);
        return "scalar-subquery#" + exprId.id + " " + s;
    }

    @Override
    public SubqueryRexNode withNewChildren(List<RexNode> children) {
        if (!children.equals(children)) {
            this.children = children;
            return new SubqueryRexNode(this.plan, this.getChildren(), this.isScalarSubquery);
        }
        return this;
    }

    public AttributeRexSet references() {
        return new AttributeRexSet(super.references().subtraction(Lists.newArrayList(plan.references().baseSet)).baseSet);
    }
}
