package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by gloway on 2019/1/9.
 */
public class RexCall extends RexNode {

    //public final RelDataType type;

    public final SqlKind sqlKind;
    public List<RexNode> operands;

    // for linkoopdb
    public RexCall(int opType, List<RexNode> operands, Type type) {
        this(SqlKind.convertOpTypes(opType), operands, type);
    }

    public RexCall(SqlKind sqlKind, List<RexNode> operands, Type type) {
        this.sqlKind = sqlKind;
        this.operands = operands;
        this.dataType = type;
        this.deterministic = SqlKind.checkDeterministic(sqlKind);
    }

    // for linkoopdb functionSqlInvoked
    public RexCall(String name, List<RexNode> operands, Type type) {
        this(SqlKind.convertByName(name), operands, type);
    }

    @Override
    public List<RexNode> getChildren() {
        return operands;
    }

    @Override
    public String simpleString() {

        if (sqlKind == SqlKind.VALUELIST) {
            return mkString(operands);
        }

        if (sqlKind == SqlKind.CASEWHEN) {
            //第一个是when 第二个是then 第三个是下一个when
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("CASE WHEN ").append(operands.get(0).simpleString())
                .append(" THEN ").append(operands.get(1).simpleString());
            if (operands.size() == 3) {
                stringBuilder.append(" ELSE ").append(operands.get(2).simpleString());
            }
            stringBuilder.append(" END");
            return stringBuilder.toString();
        } else if (operands.size() == 2) {
            return "(" + operands.get(0).simpleString() + " " + sqlKind.sql + " " + operands.get(1).simpleString() + ")";
        } else if (operands.size() == 1) {
            return sqlKind.sql + "(" + operands.get(0).simpleString() + ")";
        } else if (operands.size() > 2) {
            StringBuilder sql = new StringBuilder();
            sql.append(sqlKind.sql).append("(").append(operands.get(0).simpleString());
            for (int index = 1; index < operands.size(); index ++) {
                sql.append(", ").append(operands.get(index).simpleString());
            }
            sql.append(")");
            return sql.toString();
        }

        return super.simpleString();
    }

    @Override
    public RexCall withNewChildren(List<RexNode> children) {
        if (!operands.equals(children)) {
            this.operands = children;
            return new RexCall(this.sqlKind, this.operands, this.dataType);
        }
        return this;
    }
}
