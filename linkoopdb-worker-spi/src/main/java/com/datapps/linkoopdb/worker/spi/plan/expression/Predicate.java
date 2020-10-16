package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by gloway on 2019/1/21.
 */
public class Predicate extends RexCall {

    public Predicate(int opType, List<RexNode> operands) {
        this(SqlKind.convertOpTypes(opType), operands);
    }

    public Predicate(SqlKind sqlKind, List<RexNode> operands) {
        super(sqlKind, operands, Type.SQL_BOOLEAN);
    }

    public static Predicate equal(RexNode left, RexNode right) {
        return new Predicate(SqlKind.EQUAL, ImmutableList.of(left, right));
    }

    public static Predicate notEqual(RexNode left, RexNode right) {
        return new Predicate(SqlKind.NOT_EQUAL, ImmutableList.of(left, right));
    }

    public static RexNode and(RexNode left, RexNode right) {

        if (left == null) {
            return right;
        }

        if (right == null) {
            return left;
        }

        return new Predicate(SqlKind.AND, ImmutableList.of(left, right));
    }

    public static RexNode or(RexNode left, RexNode right) {
        if (left == null) {
            return right;
        }

        if (right == null) {
            return left;
        }
        return new Predicate(SqlKind.OR, ImmutableList.of(left, right));
    }

    public static Predicate not(RexNode node) {
        return new Predicate(SqlKind.NOT, ImmutableList.of(node));
    }

    public static Predicate greater(RexNode left, RexNode right) {
        return new Predicate(SqlKind.GREATER, ImmutableList.of(left, right));
    }

    public static Predicate greaterEqual(RexNode left, RexNode right) {
        return new Predicate(SqlKind.GREATER_EQUAL, ImmutableList.of(left, right));
    }

    public static Predicate smaller(RexNode left, RexNode right) {
        return new Predicate(SqlKind.SMALLER, ImmutableList.of(left, right));
    }

    public static Predicate smallerEqual(RexNode left, RexNode right) {
        return new Predicate(SqlKind.SMALLER_EQUAL, ImmutableList.of(left, right));
    }

    public static Predicate isNull(RexNode node) {
        return new Predicate(SqlKind.IS_NULL, ImmutableList.of(node));
    }

    public static Predicate isNotNull(RexNode node) {
        return new Predicate(SqlKind.IS_NOT_NULL, ImmutableList.of(node));
    }


    @Override
    public Predicate withNewChildren(List<RexNode> children) {
        if (!operands.equals(children)) {
            this.operands = children;
            return new Predicate(this.sqlKind, this.operands);
        }
        return this;
    }

}
