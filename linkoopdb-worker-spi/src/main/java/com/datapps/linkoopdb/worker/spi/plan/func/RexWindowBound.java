package com.datapps.linkoopdb.worker.spi.plan.func;

import java.io.Serializable;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;

/**
 * Created by gloway on 2019/1/30.
 */
public class RexWindowBound implements Serializable {

    public SqlKind sqlKind;

    private RexNode node;

    private boolean unbounded;
    private boolean preceding;
    private boolean following;
    private boolean currentRow;

    public RexWindowBound(SqlKind sqlKind) {
        this.sqlKind = sqlKind;

        switch (sqlKind) {
            case CURRENT_ROW:
                currentRow = true;
                break;
            case UNBOUNDED_PRECEDING:
                unbounded = true;
                // fall through
            case PRECEDING:
                preceding = true;
                break;
            case UNBOUNDED_FOLLOWING:
                unbounded = true;
                // fall through
            case FOLLOWING:
                following = true;
        }

    }

    public RexWindowBound(int opTypes) {
        this(SqlKind.convertOpTypes(opTypes));
    }

    public boolean isUnbounded() {
        return unbounded;
    }

    public boolean isPreceding() {
        return preceding;
    }

    public boolean isFollowing() {
        return following;
    }

    public boolean isCurrentRow() {
        return currentRow;
    }

    public RexNode getOffset() {
        return node;
    }

    public RexNode getNode() {
        return node;
    }

    public void setNode(RexNode node) {
        this.node = node;
    }

    public int getOrderKey() {
        return -1;
    }

    @Override
    public String toString() {
        return sqlKind + " " + (node == null ? "" : node.simpleString());
    }
}

