package com.datapps.linkoopdb.worker.spi.plan.func;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;

/**
 * Created by gloway on 2019/1/22.
 */
public class SqlAggFunction extends SqlFunction {

    public boolean foldable = false;
    public boolean isDistinct;
    // group_concat use
    public String separator = ",";

    public SqlAggFunction(int opType, List<RexNode> operands, Type type, boolean isDistinct) {
        this(SqlKind.convertAggregateOpTypes(opType), operands, type, isDistinct);
    }

    public SqlAggFunction(String name, List<RexNode> op, Type dataType, boolean isDistinct) {
        this(SqlKind.convertAggregateByName(name), op, dataType, isDistinct);
    }

    public SqlAggFunction(SqlKind opType, List<RexNode> operands, Type type, boolean isDistinct) {
        super(opType, operands, type);
        this.isDistinct = isDistinct;
    }

    @Override
    public SqlAggFunction withNewChildren(List<RexNode> children) {
        if (!operands.equals(children)) {
            this.operands = children;
            return new SqlAggFunction(this.sqlKind, this.operands, this.dataType, this.isDistinct);
        }
        return this;
    }
}
