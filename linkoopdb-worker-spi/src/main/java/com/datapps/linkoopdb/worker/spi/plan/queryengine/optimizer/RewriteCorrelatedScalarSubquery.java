package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.ExprId;
import com.datapps.linkoopdb.worker.spi.plan.core.JoinRelType;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbAggregate;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbFilter;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbJoin;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbProject;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.AliasRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.AttributeRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.LiteralRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.Predicate;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexCall;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;
import com.datapps.linkoopdb.worker.spi.plan.expression.SubqueryRexNode;
import com.datapps.linkoopdb.worker.spi.plan.func.SqlAggFunction;
import com.datapps.linkoopdb.worker.spi.plan.util.Utilities;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

public class RewriteCorrelatedScalarSubquery extends Rule {

    @Override
    public RelNode process(RelNode relNode) {
        return relNode.transform((rel) -> {
            if (rel instanceof LdbAggregate) {
                List<SubqueryRexNode> subqueries = Lists.newArrayList();
                List newAggregateExpr = ((LdbAggregate) rel).expressions().stream().map(expr -> extractCorrelatedScalarSubqueries(expr, subqueries)).collect(
                    Collectors.toList());
                if (!subqueries.isEmpty()) {
                    List<RexNode> newgroupingExpr = ((LdbAggregate) rel).groupingExpressions.stream().flatMap(e ->
                        subqueries.stream().filter(subquery -> !subquery.semanticEquals(e)).map(subquery -> subquery.plan.getOutput().get(0))
                    ).collect(Collectors.toList());
                    return new LdbAggregate(newgroupingExpr, newAggregateExpr, constructeLeftJoins(((LdbAggregate) rel).child, subqueries));
                }
                return rel;
            } else if (rel instanceof LdbProject) {
                List<SubqueryRexNode> subqueries = Lists.newArrayList();
                List<RexNode> newExpression = ((LdbProject) rel).expressions().stream().map(expr -> extractCorrelatedScalarSubqueries(expr, subqueries))
                    .collect(Collectors.toList());
                if (!subqueries.isEmpty()) {
                    return new LdbProject(newExpression, constructeLeftJoins(((LdbProject) rel).child, subqueries));
                }
                return rel;
            } else if (rel instanceof LdbFilter) {
                List<SubqueryRexNode> subqueries = Lists.newArrayList();
                RexNode newCondition =  ((LdbFilter) rel).expressions().stream().map(expr -> extractCorrelatedScalarSubqueries(expr, subqueries)).collect(
                    Collectors.toList()).get(0);
                if (!subqueries.isEmpty()) {
                    return new LdbProject(((LdbFilter) rel).getOutput(), new LdbFilter(newCondition, constructeLeftJoins(((LdbFilter) rel).child, subqueries)));
                }
                return rel;
            }
            return rel;
        });
    }

    private RelNode constructeLeftJoins(RelNode child, List<SubqueryRexNode> subqueryRexNodeList) {
        return Utilities.foldLeft(subqueryRexNodeList, child, (relNode, subqueryRexNode) -> {
            RelNode query = subqueryRexNode.plan;
            List<RexNode> conditions = subqueryRexNode.getChildren();
            RexNode origOutput = query.output.get(0);
            Optional resultWithZeroTups = evalSubqueryOnZeroTups(query);
            if (!resultWithZeroTups.isPresent()) {
                LdbJoin ldbJoin = new LdbJoin(child, query, JoinRelType.LEFT_OUTER, Utilities.reduceLeftPredicates(conditions));
                return new LdbProject(Utilities.addElementToLast(child.getOutput(), origOutput), ldbJoin);
            } else {
                Triple<List<RelNode>, Optional<LdbFilter>, LdbAggregate> triple = splitSubquery(query);
                List<RelNode> topPart = triple.getLeft();
                Optional<LdbFilter> havingNode = triple.getMiddle();
                LdbAggregate aggNode = triple.getRight();
                ExprId alwaysTrueExprId = NamedRexNode.newExprId();
                AliasRexNode alwaysTrueExpr = new AliasRexNode(new LiteralRexNode(true, Type.SQL_BOOLEAN), "alwaysTrue", alwaysTrueExprId);
                AttributeRexNode alwaysTrueRef = new AttributeRexNode("alwaysTrue", Type.SQL_BOOLEAN, false, alwaysTrueExprId, ImmutableList.of());
                RexNode aggValRef = query.output.get(0);
                if (!triple.getMiddle().isPresent()) {
                    LdbJoin leftOuterJoin = new LdbJoin(child, new LdbProject(Utilities.addElementToLast(query.getOutput(), alwaysTrueExpr), query),
                        JoinRelType.LEFT_OUTER, Utilities.reduceLeftPredicates(conditions));
                    RexCall isNull = new RexCall(SqlKind.IS_NULL, ImmutableList.of(alwaysTrueRef), Type.SQL_BOOLEAN);
                    LiteralRexNode literalSubquy = new LiteralRexNode(resultWithZeroTups.get(), origOutput.dataType);
                    RexCall ifRexCall = new RexCall(SqlKind.IF, ImmutableList.of(isNull, literalSubquy, aggValRef), aggValRef.dataType);
                    AliasRexNode aliasRexNode = new AliasRexNode(ifRexCall, ((NamedRexNode) origOutput).name, ((NamedRexNode) origOutput).exprId);
                    LdbProject ldbProject = new LdbProject(Utilities.addElementToLast(child.getOutput(), aliasRexNode), leftOuterJoin);
                    return ldbProject;
                } else {
                    RelNode subqueryRoot = aggNode;
                    List<RexNode> havingInputs = aggNode.output;
                    Collections.reverse(topPart);
                    for (RelNode rel : topPart) {
                        if (rel instanceof LdbProject) {
                            subqueryRoot = new LdbProject(Utilities.addAll(rel.expressions(), havingInputs), subqueryRoot);
                            return subqueryRoot;
                        }
                        throw new RuntimeException("Unexpected operator in corelated subquery");
                    }
                    RexCall isNull = new RexCall(SqlKind.IS_NULL, ImmutableList.of(alwaysTrueRef), Type.SQL_BOOLEAN);
                    LiteralRexNode literalSubquy = new LiteralRexNode(resultWithZeroTups.get(), origOutput.dataType);

                    Predicate not = new Predicate(SqlKind.NOT, ImmutableList.of(havingNode.get().expressions().get(0)));
                    LiteralRexNode literalNull = new LiteralRexNode(null, aggValRef.dataType);

                    RexCall caseWhenRexCall1 = new RexCall(SqlKind.CASEWHEN, ImmutableList.of(not, literalNull, aggValRef), aggValRef.dataType);
                    RexCall caseWhenRexCall2 = new RexCall(SqlKind.CASEWHEN, ImmutableList.of(isNull, literalSubquy, caseWhenRexCall1), aggValRef.dataType);

                    AliasRexNode aliasRexNode = new AliasRexNode(caseWhenRexCall2, ((NamedRexNode) origOutput).name, ((NamedRexNode) origOutput).exprId);
                    LdbProject ldbProject = new LdbProject(Utilities.addElementToLast(subqueryRoot.getOutput(), alwaysTrueExpr), subqueryRoot);
                    LdbJoin ldbJoin = new LdbJoin(child, ldbProject, JoinRelType.LEFT_OUTER, Utilities.reduceLeftPredicates(conditions));
                    return new LdbProject(Utilities.addElementToLast(subqueryRoot.getOutput(), aliasRexNode), ldbJoin);
                }
            }
        });
    }

    private Triple<List<RelNode>, Optional<LdbFilter>, LdbAggregate> splitSubquery(RelNode plan) {
        List<RelNode> topPart = Lists.newArrayList();
        RelNode botoomPart = plan;
        while (true) {
            if (botoomPart instanceof LdbFilter && ((LdbFilter) botoomPart).child instanceof LdbAggregate) {
                return Triple.of(topPart, Optional.of((LdbFilter) botoomPart), (LdbAggregate) ((LdbFilter) botoomPart).child);
            } else if (botoomPart instanceof LdbAggregate) {
                return Triple.of(topPart, Optional.of(null), (LdbAggregate) botoomPart);
            } else if (botoomPart instanceof LdbProject) {
                topPart.add(botoomPart);
                botoomPart = ((LdbProject) botoomPart).child;
            } else if (botoomPart instanceof LdbFilter) {
                throw new RuntimeException("Correlated subquery has unexpected operator below filter");
            }
            throw new RuntimeException("Unexpected operator in correlated subquery");
        }
    }


    private Optional<RexNode> evalSubqueryOnZeroTups(RelNode relNode) {
        Map<ExprId, Optional<RexNode>> resultMap = evalPlan(relNode);
        ExprId scalarValueExprId = ((NamedRexNode) relNode.output.get(0)).exprId;
        if (resultMap.containsKey(scalarValueExprId)) {
            return resultMap.get(scalarValueExprId);
        }
        return Optional.ofNullable(null);
    }

    private Map<ExprId, Optional<RexNode>> evalPlan(RelNode relNode) {
        if (relNode instanceof LdbFilter) {
            Map<ExprId, Optional<RexNode>> bindings = evalPlan(((LdbFilter) relNode).child);
            if (bindings.isEmpty()) {
                return bindings;
            } else {
                if (evalExpr(relNode.expressions().get(0), bindings).isPresent()) {
                    return bindings;
                }
                return Maps.newHashMap();
            }
        } else if (relNode instanceof LdbProject) {
            Map<ExprId, Optional<RexNode>> bindings = evalPlan(((LdbProject) relNode).child);
            if (bindings.isEmpty()) {
                return bindings;
            } else {
                return relNode.expressions().stream().map(ne -> Pair.of(((NamedRexNode) ne).exprId, evalExpr(ne, bindings)))
                    .collect(Collectors.toMap(pair -> pair.getKey(), pair -> pair.getValue()));
            }

        } else if (relNode instanceof LdbAggregate) {
            ((LdbAggregate) relNode).aggregateExpressions.stream().map(aggExpr -> {
                if (aggExpr instanceof AttributeRexNode) {
                    return Pair.of(aggExpr.exprId, Optional.ofNullable(null));
                } else if (aggExpr instanceof AliasRexNode && ((AliasRexNode) aggExpr).child instanceof AttributeRexNode) {
                    return Pair.of(aggExpr.exprId, Optional.ofNullable(null));
                }
                return Pair.of(aggExpr.exprId, evalAggOnZeroTups(aggExpr));
            }).collect(Collectors.toMap(pair -> pair.getKey(), pair -> pair.getValue()));
        } else {
            throw new RuntimeException("Unexpected operator in scalar subquery");
        }
        return Maps.newHashMap();
    }

    private Optional<RexNode> evalAggOnZeroTups(RexNode rexNode) {
        return Optional.of(rexNode.transform(rex -> {
            if (rex instanceof SqlAggFunction || rex instanceof AttributeRexNode) {
                return new LiteralRexNode(null, Type.ANYTYPE);
            }
            return rex;
        }));
    }

    private Optional<RexNode> evalExpr(RexNode rexNode, Map<ExprId, Optional<RexNode>> bindings) {
        return Optional.of(rexNode.transform(rex -> {
            if (rex instanceof AttributeRexNode) {
                if (bindings.containsKey(((AttributeRexNode) rex).exprId)) {
                    return new LiteralRexNode(bindings.get(((AttributeRexNode) rex).exprId), ((AttributeRexNode) rex).dataType);
                }
                return new LiteralRexNode(0, ((AttributeRexNode) rex).dataType);
            }
            return rex;
        }));
    }


    private RexNode extractCorrelatedScalarSubqueries(RexNode rexNode, List<SubqueryRexNode> subqueryRexNodeList) {
        return rexNode.transform(rex -> {
            if (rex instanceof SubqueryRexNode && ((SubqueryRexNode) rex).isScalarSubquery && !rex.getChildren().isEmpty()) {
                subqueryRexNodeList.add((SubqueryRexNode) rex);
                return ((SubqueryRexNode) rex).plan.getOutput().get(0);
            }
            return rex;
        });
    }
}
