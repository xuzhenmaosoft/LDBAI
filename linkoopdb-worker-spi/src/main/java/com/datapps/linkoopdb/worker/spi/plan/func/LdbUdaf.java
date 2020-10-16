package com.datapps.linkoopdb.worker.spi.plan.func;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;

/**
 * Created by gloway on 2019/12/25.
 */
public class LdbUdaf extends SqlAggFunction {

    public String functionName;
    public List<Type> parTypes;

    public LdbUdaf(String functionName, List<RexNode> operands, List<Type> parTypes, Type type) {
        super(SqlKind.UDAF, operands, type, false);
        this.functionName = functionName;
        this.parTypes = parTypes;
    }
}
