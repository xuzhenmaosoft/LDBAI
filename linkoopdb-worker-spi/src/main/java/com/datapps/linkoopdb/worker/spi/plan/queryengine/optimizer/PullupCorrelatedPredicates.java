package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datapps.linkoopdb.worker.spi.plan.core.LdbAggregate;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbFilter;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbProject;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.core.SingleRel;
import com.datapps.linkoopdb.worker.spi.plan.expression.AttributeRexSet;
import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.Predicate;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexOuterRef;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;
import com.datapps.linkoopdb.worker.spi.plan.expression.SubqueryRexNode;
import com.datapps.linkoopdb.worker.spi.plan.util.Utilities;
import org.apache.commons.lang3.tuple.Pair;

public class PullupCorrelatedPredicates extends Rule {

    @Override
    public RelNode process(RelNode relNode) {
        return relNode.transformUp((rel) -> {
            if (rel instanceof SingleRel) {
                if (rel instanceof LdbFilter && rel.getChildren().size() == 1 && rel.getChildren().get(0) instanceof LdbAggregate) {
                    LdbAggregate ldbAggregate = (LdbAggregate) rel.getChildren().get(0);
                    return rewriteSubQueries((SingleRel) rel, Lists.newArrayList(ldbAggregate, ldbAggregate.child));
                } else {
                    return rewriteSubQueries((SingleRel) rel, rel.getChildren());
                }
            }
            return rel;
        });
    }

    public RelNode rewriteSubQueries(RelNode plan, List<RelNode> outerPlans) {
        return plan.transformExpression(expr -> {
            if (expr instanceof Predicate) {
                Map<RelNode, List<RexNode>> predicateMap = Maps.newHashMap();
                if (((Predicate) expr).sqlKind == SqlKind.EXISTS && expr.getChildren().get(0) instanceof SubqueryRexNode) {
                    SubqueryRexNode subqueryRexNode = (SubqueryRexNode) expr.getChildren().get(0);
                    RelNode sub = subqueryRexNode.plan;
                    List children = subqueryRexNode.getChildren();
                    Pair<RelNode, List<RexNode>> pair = pullOutCorrelatedPredicates(sub, children, predicateMap);
                    SubqueryRexNode newSubqueryRexNode = new SubqueryRexNode(pair.getLeft(), pair.getRight(), subqueryRexNode.isScalarSubquery);
                    return new Predicate(55, ImmutableList.of(newSubqueryRexNode));

                } else if (((Predicate) expr).sqlKind == SqlKind.IN && expr.getChildren().get(1) instanceof SubqueryRexNode) {
                    SubqueryRexNode subqueryRexNode = (SubqueryRexNode) expr.getChildren().get(1);
                    RelNode sub = subqueryRexNode.plan;
                    List children = subqueryRexNode.getChildren();
                    Pair<RelNode, List<RexNode>> pair = pullOutCorrelatedPredicates(sub, children, predicateMap);
                    SubqueryRexNode newSubqueryRexNode = new SubqueryRexNode(pair.getLeft(), pair.getRight(), subqueryRexNode.isScalarSubquery);
                    return new Predicate(54, ImmutableList.of(((Predicate) expr).getChildren().get(0), newSubqueryRexNode));

                }
            } else if (expr instanceof SubqueryRexNode && ((SubqueryRexNode) expr).isScalarSubquery == true) {
                Map predicateMap = Maps.newHashMap();
                RelNode sub = ((SubqueryRexNode) expr).plan;
                List children = expr.getChildren();
                Pair<RelNode, List<RexNode>> pair = pullOutCorrelatedPredicates(sub, children, predicateMap);
                SubqueryRexNode newSubqueryRexNode = new SubqueryRexNode(pair.getLeft(), pair.getRight(), ((SubqueryRexNode) expr).isScalarSubquery);
                return newSubqueryRexNode;
            }
            return expr;
        });
    }

    /**
     * 找到子查询中的相关谓词,上拉
     */
    public Pair<RelNode, List<RexNode>> pullOutCorrelatedPredicates(RelNode sub, List<RelNode> outer, Map<RelNode, List<RexNode>> predicateMap) {
        RelNode transformed = sub.transformUp(rel -> {
            if (rel instanceof LdbFilter) {

                List<RexNode> allExpression = Lists.newArrayList();
                Utilities.splitConjunctivePredicates(((LdbFilter) rel).expressions().get(0), allExpression);

                Map<Boolean, List<RexNode>> resMap = allExpression.stream().collect(Collectors.partitioningBy(expr -> {
                    return expr.find(e -> e instanceof RexOuterRef) != null;
                }));

                List correlated = resMap.get(true);
                List local = resMap.get(false);

                if (correlated.size() == 0) {
                    return rel;
                } else if (!local.isEmpty()) {
                    LdbFilter newFilter = new LdbFilter(Utilities.reduceLeftPredicates(local), ((LdbFilter) rel).child);
                    predicateMap.put(newFilter, correlated);
                    return newFilter;
                } else {
                    predicateMap.put(((LdbFilter) rel).child, correlated);
                    return ((LdbFilter) rel).child;
                }

            } else if (rel instanceof LdbProject) {
                AttributeRexSet referencesToAdd = missingReferences((LdbProject) rel, predicateMap);

                if (referencesToAdd.getSize() != 0) {
                    List referencesToAddList = Lists.newArrayList(referencesToAdd.getBaseSet());
                    return new LdbProject(Utilities.addAll(((LdbProject) rel).expressions(), referencesToAddList), ((LdbProject) rel).child);
                }
            } else if (rel instanceof LdbAggregate) {
                AttributeRexSet referencesToAdd = missingReferences((LdbAggregate) rel, predicateMap);

                if (referencesToAdd.getSize() != 0) {
                    List referencesToAddList = Lists.newArrayList(referencesToAdd.getBaseSet());
                    return new LdbAggregate(Utilities.addAll(((LdbAggregate) rel).groupingExpressions, referencesToAddList),
                        Utilities.addAll(((LdbAggregate) rel).aggregateExpressions, referencesToAddList),
                        ((LdbAggregate) rel).child
                    );
                }
            }
            return rel;
        });
        // TODO Make sure the inner and the outer query attributes do not collide.

        return Pair.of(transformed, Utilities.stripOuterReferences(predicateMap.values()
            .stream().flatMap(predicate -> predicate.stream()).collect(toList())));
    }

    /**
     * 判断子查询的output是否缺少相关谓词所需要的AttributeReference
     */
    public AttributeRexSet missingReferences(RelNode relNode, Map predicateMap) {
        List flatten = (List) relNode.collect(rel -> {
            if (predicateMap.containsKey(rel)) {
                return Optional.of(predicateMap.get(rel));
            } else {
                return Optional.empty();
            }
        }).stream().flatMap(ele -> {
            if (ele instanceof List) {
                return ((List) ele).stream();
            }
            return ele;
        }).collect(toList());

        AttributeRexSet localPredicateReferences = (AttributeRexSet) flatten.stream().map(ele -> {
            if (ele instanceof RexNode) {
                return ((RexNode) ele).references();
            }
            return ele;
        }).reduce((a1, a2) -> {
            if (a1 instanceof AttributeRexSet && a2 instanceof AttributeRexSet) {
                return ((AttributeRexSet) a1).plusplus((AttributeRexSet) a2);
            }
            return a1;
        }).orElse(new AttributeRexSet(Sets.newHashSet()));

        return localPredicateReferences.subtraction((List<NamedRexNode>) (List) relNode.getOutput());
    }
}
