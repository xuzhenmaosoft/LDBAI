package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

/**
 * Created by gloway on 2019/1/16.
 */
public class SortOrderRexNode extends SingleRexNode {

    public boolean ascending;
    public boolean nullFirst;

    public SortOrderRexNode(RexNode child, boolean ascending, boolean nullFirst) {
        super(child);
        this.ascending = ascending;
        this.nullFirst = nullFirst;
    }

    @Override
    public SortOrderRexNode withNewChildren(List<RexNode> children) {
        if (!child.equals(children.get(0))) {
            this.child = children.get(0);
            return new SortOrderRexNode(child, this.ascending, this.nullFirst);
        }
        return this;
    }

    @Override
    public String simpleString() {
        return child.simpleString()
            + (ascending ? " Asc" : " Desc")
            + (nullFirst ? " Nulls First" : " Nulls Last");
    }
}
