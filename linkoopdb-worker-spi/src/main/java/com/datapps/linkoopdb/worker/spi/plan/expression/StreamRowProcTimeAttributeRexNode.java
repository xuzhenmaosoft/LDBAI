package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;
import java.util.Objects;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.ExprId;

/**
 * 对于具有rowtime或者proctime字段的流表，需要使用原始name，而不能增加exprid。
 */
public class StreamRowProcTimeAttributeRexNode extends AttributeRexNode {

    public StreamRowProcTimeAttributeRexNode(String name, Type dataType) {
        super(name, dataType);
        this.exprId = NamedRexNode.newExprId();
    }

    public StreamRowProcTimeAttributeRexNode(String name, Type dataType, boolean nullable, ExprId exprId, List<String> qualifier) {
        super(name, dataType, nullable, exprId, qualifier);
    }

    @Override
    public String simpleString() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AttributeRexNode) {
            AttributeRexNode ar = (AttributeRexNode) obj;
            return Objects.equals(name, ar.name) && dataType.equals(ar.dataType) && nullable == ar.nullable
                ;
        } else {
            return false;
        }
    }
    public int hashCode() {
        return name.hashCode();
    }

}
