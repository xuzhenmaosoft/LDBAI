package com.datapps.linkoopdb.worker.spi.plan.func;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexCall;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;

/**
 * Created by gloway on 2019/1/22.
 */
public class SqlFunction extends RexCall {

    public SqlFunction(int funcType, List<RexNode> operands, Type type) {
        super(SqlKind.convertSqlFunctionSQL(funcType), operands, type);
    }

    public SqlFunction(SqlKind sqlKind, List<RexNode> operands, Type type) {
        super(sqlKind, operands, type);
    }

    @Override
    public SqlFunction withNewChildren(List<RexNode> children) {
        if (!operands.equals(children)) {
            this.operands = children;
            return new SqlFunction(this.sqlKind, this.operands, this.dataType);
        }
        return this;
    }
}
