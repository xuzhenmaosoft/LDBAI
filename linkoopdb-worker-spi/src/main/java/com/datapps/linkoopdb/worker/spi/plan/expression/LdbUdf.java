package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by gloway on 2019/4/2.
 */
public class LdbUdf extends RexCall {

    public String functionName;
    public List<Type> parTypes;

    public LdbUdf(String functionName, List<RexNode> operands, List<Type> parTypes, Type type) {
        super(SqlKind.UDF, operands, type);
        this.functionName = functionName;
        this.parTypes = parTypes;
    }

    @Override
    public String simpleString() {
        return "UDF: " + functionName + mkString(operands);
    }
}
