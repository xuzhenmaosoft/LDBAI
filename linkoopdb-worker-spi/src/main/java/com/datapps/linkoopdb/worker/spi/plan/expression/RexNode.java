package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.stream.Collectors;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.TreeNode;

/**
 * Created by gloway on 2019/1/8.
 */
public abstract class RexNode extends TreeNode<RexNode> {


    public Type dataType;

    public boolean foldable;
    public boolean nullable;

    protected Boolean deterministic = null;

    public Boolean deterministic() {
        if (deterministic == null) {
            deterministic = getChildren().stream().allMatch(RexNode::deterministic);
        }
        return deterministic;
    }


    @Override
    protected String argString() {
        return "";
    }

    public RexNode withNullability(boolean newNullability) {
        if (nullable != newNullability) {
            this.nullable = newNullability;
        }
        return this;
    }

    public AttributeRexSet references() {
        return new AttributeRexSet(getChildren().stream().flatMap(child -> {
            return child.references().getBaseSet().stream();
        }).collect(Collectors.toSet()));
    }

    public boolean semanticEquals(RexNode other) {
        if (this.deterministic == other.deterministic && this.canonicalized().equals(other.canonicalized())) {
            return true;
        }
        return false;
    }

    // TODO rexnode canonicalized
    public RexNode canonicalized() {
        return this;
    }
}
