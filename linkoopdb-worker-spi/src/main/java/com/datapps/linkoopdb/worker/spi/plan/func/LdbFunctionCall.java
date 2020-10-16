package com.datapps.linkoopdb.worker.spi.plan.func;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexCall;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;

/**
 * @author xingbu
 * @version 1.0
 *
 *          created by　19-3-22 下午12:15
 */
public class LdbFunctionCall extends RexCall {

    public RelNode invoker;

    public List<String> parameter;

    public LdbFunctionCall(SqlKind sqlKind, List<String> parameter,
        List<RexNode> operands, Type type) {
        super(sqlKind, operands, type);
        this.parameter = parameter;
    }

    @Override
    public String simpleString() {
        return sqlKind.sql;
    }

    @Override
    public RexCall withNewChildren(List<RexNode> children) {
        this.operands = children;
        return new LdbFunctionCall(this.sqlKind, this.parameter, this.operands, this.dataType);
    }
}
