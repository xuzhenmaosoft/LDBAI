package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.ArrayList;
import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by gloway on 2019/3/13.
 */
public class NamedStructRexNode extends RexNode {

    public List<RexNode> valueList;
    public List<RexNode> nameList;

    public NamedStructRexNode(List<RexNode> valueList) {
        List<RexNode> nameList = new ArrayList<>();
        for (int i = 0; i < valueList.size(); i++) {
            nameList.add(new LiteralRexNode("col_" + i, Type.SQL_VARCHAR));
        }
        this.nameList = nameList;
        this.valueList = valueList;
        dataType = Type.SQL_ARRAY_ALL_TYPES;
    }

    public NamedStructRexNode(List<RexNode> nameList, List<RexNode> valueList) {
        this.nameList = nameList;
        this.valueList = valueList;
        dataType = Type.SQL_ARRAY_ALL_TYPES;
    }

    @Override
    public List<RexNode> getChildren() {
        ArrayList<RexNode> children = new ArrayList<>();
        for (int i = 0; i < valueList.size(); i++) {
            children.add(nameList.get(i));
            children.add(valueList.get(i));
        }
        return children;
    }

    @Override
    public RexNode withNewChildren(List<RexNode> children) {
        return new NamedStructRexNode(children);
    }
}
