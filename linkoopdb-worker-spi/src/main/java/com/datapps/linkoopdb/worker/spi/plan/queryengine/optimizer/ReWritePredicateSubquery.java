package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.codepoetics.protonpack.StreamUtils;
import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.core.JoinRelType;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbFilter;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbJoin;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbProject;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.AttributeRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.Predicate;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;
import com.datapps.linkoopdb.worker.spi.plan.expression.SubqueryRexNode;
import com.datapps.linkoopdb.worker.spi.plan.util.Utilities;
import org.apache.commons.lang3.tuple.Pair;

public class ReWritePredicateSubquery extends Rule {

    @Override
    public RelNode process(RelNode relNode) {
        return relNode.transform((rel) -> {
            if (rel instanceof LdbFilter) {

                // 按照是否有子查询,将condition分为两组
                List<RexNode> allCondition = Lists.newArrayList();
                Utilities.splitConjunctivePredicates(((LdbFilter) rel).expressions().get(0), allCondition);

                Map<Boolean, List<RexNode>> resMap = allCondition.stream().collect(Collectors.partitioningBy(condition -> {
                    return (condition.find(expr -> {
                        if (expr instanceof Predicate) {
                            if (((Predicate) expr).sqlKind == SqlKind.EXISTS || ((Predicate) expr).sqlKind == SqlKind.IN) {
                                return true;
                            }
                        }
                        return false;
                    }) != null);
                }));

                List<RexNode> withSubquery = resMap.get(true);
                List<RexNode> withoutSubquery = resMap.get(false);

                RelNode newFilter = withoutSubquery.isEmpty() ? ((LdbFilter) rel).child :
                    new LdbFilter(Utilities.reduceLeftPredicates(withoutSubquery), ((LdbFilter) rel).child);

                // 针对Exists/IN/Not Exists/Not IN等分别做处理
                for (RexNode rexNode : withSubquery) {
                    if (rexNode instanceof Predicate) {
                        if (((Predicate) rexNode).sqlKind == SqlKind.EXISTS && rexNode.getChildren().get(0) instanceof SubqueryRexNode) {
                            RelNode sub = ((SubqueryRexNode) rexNode.getChildren().get(0)).plan;
                            List<RexNode> conditions = ((SubqueryRexNode) rexNode.getChildren().get(0)).getChildren();

                            Pair<RexNode, RelNode> rexNodeRelNodePair = rewriteExistentialExpr(conditions, newFilter);
                            RelNode join = new LdbJoin(rexNodeRelNodePair.getRight(), sub, JoinRelType.LEFT_SEMI, rexNodeRelNodePair.getLeft());
                            return newFilter = join;

                        } else if (((Predicate) rexNode).sqlKind == SqlKind.IN && rexNode.getChildren().get(1) instanceof SubqueryRexNode) {

                            RelNode sub = ((SubqueryRexNode) rexNode.getChildren().get(1)).plan;
                            List inConditions = StreamUtils
                                .zip(Arrays.asList(((Predicate) rexNode).operands.get(0)).stream(), sub.getOutput().stream(), (outer, inner) ->
                                    new Predicate(SqlKind.EQUAL, ImmutableList.of(outer, inner))).collect(Collectors.toList());
                            List<RexNode> conditions = ((SubqueryRexNode) rexNode.getChildren().get(1)).getChildren();

                            Pair<RexNode, RelNode> rexNodeRelNodePair = rewriteExistentialExpr(Utilities.addAll(conditions, inConditions), newFilter);
                            RelNode join = new LdbJoin(rexNodeRelNodePair.getRight(), sub, JoinRelType.LEFT_SEMI, rexNodeRelNodePair.getLeft());
                            return newFilter = join;

                        } else if (((Predicate) rexNode).sqlKind == SqlKind.NOT) {
                            if (((Predicate) rexNode).operands != null && !((Predicate) rexNode).operands.isEmpty()) {
                                if (((Predicate) rexNode).operands.get(0) instanceof Predicate) {
                                    Predicate notPredicate = (Predicate) ((Predicate) rexNode).operands.get(0);

                                    if (notPredicate.sqlKind == SqlKind.EXISTS
                                        && notPredicate.getChildren().get(0) instanceof SubqueryRexNode) {
                                        RelNode sub = ((SubqueryRexNode) notPredicate.getChildren().get(0)).plan;
                                        List<RexNode> conditions = notPredicate.getChildren().get(0).getChildren();

                                        Pair<RexNode, RelNode> rexNodeRelNodePair = rewriteExistentialExpr(conditions, newFilter);
                                        RelNode join = new LdbJoin(rexNodeRelNodePair.getRight(), sub, JoinRelType.LEFT_ANTI, rexNodeRelNodePair.getLeft());
                                        return newFilter = join;

                                    } else if (notPredicate.sqlKind == SqlKind.IN
                                        && notPredicate.getChildren().get(1) instanceof SubqueryRexNode) {
                                        RelNode sub = ((SubqueryRexNode) notPredicate.getChildren().get(1)).plan;
                                        List inConditions = StreamUtils
                                            .zip(Arrays.asList(notPredicate.operands.get(0)).stream(), sub.getOutput().stream(), (outer, inner) ->
                                                new Predicate(SqlKind.EQUAL, ImmutableList.of(outer, inner))).collect(Collectors.toList());

                                        List<RexNode> conditions = notPredicate.getChildren().get(1).getChildren();

                                        Pair<RexNode, RelNode> rexNodeRelNodePair = rewriteExistentialExpr(Utilities.addAll(conditions, inConditions),
                                            newFilter);
                                        RelNode join = new LdbJoin(rexNodeRelNodePair.getRight(), sub, JoinRelType.LEFT_ANTI, rexNodeRelNodePair.getLeft());
                                        return newFilter = join;
                                    }
                                }
                            }
                        } else {
                            Pair<RexNode, RelNode> rexNodeRelNodePair = rewriteExistentialExpr(((Predicate) rexNode).operands, newFilter);
                            RelNode project = new LdbProject(newFilter.getOutput(), new LdbFilter(rexNodeRelNodePair.getLeft(), rexNodeRelNodePair.getRight()));
                            return newFilter = project;
                        }
                    }
                }


            }
            return rel;
        });
    }

    public Pair<RexNode, RelNode> rewriteExistentialExpr(List<RexNode> exprs, RelNode plan) {
        RelNode newPlan = plan;
        RelNode[] relNodes = new RelNode[1];
        relNodes[0] = newPlan;
        exprs.forEach(expr -> {
            expr.transformUp(e -> {
                if (e instanceof Predicate && ((Predicate) e).sqlKind == SqlKind.EXISTS
                    && e.getChildren().get(0) instanceof SubqueryRexNode) {
                    AttributeRexNode exists = new AttributeRexNode("exists", Type.SQL_BOOLEAN, false, NamedRexNode.newExprId(), ImmutableList.of());
                    RelNode sub = ((SubqueryRexNode) e.getChildren().get(0)).plan;
                    List<RexNode> conditions = ((SubqueryRexNode) e.getChildren().get(0)).getChildren();

                    RelNode join = new LdbJoin(relNodes[0], sub, JoinRelType.EXISTENCEJOIN, Utilities.reduceLeftPredicates(conditions));
                    ((LdbJoin) join).setExistenceJoinAttr(exists);
                    relNodes[0] = join;
                    return exists;
                } else if (e instanceof Predicate && ((Predicate) e).sqlKind == SqlKind.IN
                    && e.getChildren().get(0) instanceof SubqueryRexNode) {
                    AttributeRexNode exists = new AttributeRexNode("exists", Type.SQL_BOOLEAN, false, NamedRexNode.newExprId(), ImmutableList.of());
                    RelNode sub = ((SubqueryRexNode) e.getChildren().get(0)).plan;
                    List<RexNode> conditions = ((SubqueryRexNode) e.getChildren().get(0)).getChildren();

                    RelNode join = new LdbJoin(relNodes[0], sub, JoinRelType.EXISTENCEJOIN, Utilities.reduceLeftPredicates(conditions));
                    ((LdbJoin) join).setExistenceJoinAttr(exists);
                    relNodes[0] = join;

                    return exists;
                }
                return e;
            });
        });
        return Pair.of(Utilities.reduceLeftPredicates(exprs), relNodes[0]);
    }

}
