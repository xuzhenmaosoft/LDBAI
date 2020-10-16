package com.datapps.linkoopdb.worker.spi.plan.queryengine;

import java.util.ArrayList;
import java.util.List;

import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.CombineUnions;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.PullupCorrelatedPredicates;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.ReWritePredicateSubquery;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.ReplaceDistinctWithAggregate;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.ReplaceExceptWithAntiJoin;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.ReplaceIntersectWithSemiJoin;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.RewriteCorrelatedScalarSubquery;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.RewriteExceptAll;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.RewriteIntersectAll;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LdbOptimizer {

    protected static final Logger logger = LoggerFactory.getLogger(LdbOptimizer.class);


    List<Batch> batches = new ArrayList<>();

    public LdbOptimizer() {
        Batch unionOpt = new Batch("optUnion", 10);
        Batch finshAnanlyze = new Batch("Finish Analysis", 1);
        Batch replaceOperators = new Batch("Replace Operators", 100);
        Batch rulesWithoutInferFiltersFromConstraints = new Batch("Operator Optimization before Inferring Filters", 100);
        Batch pullupCorrelatedExpressions = new Batch("Pullup Correlated Expressions", 1);
        Batch rewriteSubquery = new Batch("RewriteSubquery", 1);

        ReplaceDistinctWithAggregate replaceDistinctAggregates = new ReplaceDistinctWithAggregate();
        ReplaceExceptWithAntiJoin replaceExceptWithAntiJoin = new ReplaceExceptWithAntiJoin();
        ReplaceIntersectWithSemiJoin replaceIntersectWithSemiJoin = new ReplaceIntersectWithSemiJoin();
        RewriteExceptAll rewriteExceptAll = new RewriteExceptAll();
        RewriteIntersectAll rewriteIntersectAll = new RewriteIntersectAll();
        PullupCorrelatedPredicates pullupCorrelatedPredicates = new PullupCorrelatedPredicates();
        ReWritePredicateSubquery reWritePredicateSubquery = new ReWritePredicateSubquery();
        RewriteCorrelatedScalarSubquery rewriteCorrelatedScalarSubquery = new RewriteCorrelatedScalarSubquery();

        CombineUnions combineUnions = new CombineUnions();
        unionOpt.rules.add(combineUnions);
        replaceOperators.rules.add(rewriteExceptAll);
        replaceOperators.rules.add(replaceIntersectWithSemiJoin);
        replaceOperators.rules.add(replaceExceptWithAntiJoin);
        replaceOperators.rules.add(rewriteIntersectAll);
        replaceOperators.rules.add(replaceDistinctAggregates);
        pullupCorrelatedExpressions.rules.add(pullupCorrelatedPredicates);
        rewriteSubquery.rules.add(reWritePredicateSubquery);
        // rewriteSubquery.rules.add(rewriteCorrelatedScalarSubquery);

        /*batches.add(pullupCorrelatedExpressions);
        batches.add(rewriteSubquery);
        batches.add(replaceOperators);
        batches.add(finshAnanlyze);
        batches.add(rulesWithoutInferFiltersFromConstraints);*/
        batches.add(unionOpt);

    }


    public RelNode optimizeRelNode(RelNode relNode) {
        RelNode curRelNode = relNode;
        try {
            for (Batch batch : batches) {
                boolean whileFlag = true;
                int iteration = 1;
                List<Rule> rules = batch.rules;

                String beforeBtachPlan = relNode.toString();
                while (whileFlag) {

                    String curPlan = relNode.toString();
                    for (Rule rule : rules) {
                        String beforeOptPlan = relNode.toString();
                        RelNode afterRule = curRelNode;
                        afterRule = rule.process(curRelNode);
                        String afterOptSql = afterRule.toString();
                        if (!beforeOptPlan.equals(afterOptSql)) {
                            curRelNode = afterRule;
                            logger.info("Applying Rule " + rule.ruleName);
                            logger.info(beforeOptPlan);
                            logger.info(afterOptSql);
                        }
                    }

                    iteration++;
                    if (iteration > batch.maxIterations) {
                        if (iteration != 2) {
                            String message = "Max iterations " + (iteration - 1) + "reached for batch " + batch.name;
                            logger.warn(message);
                        }
                        whileFlag = false;
                    }

                    String lastPlan = relNode.toString();

                    if (curPlan.equals(lastPlan)) {
                        logger.trace("Fixed point reached for batch " + batch.name + " after " + (iteration - 1) + " iterations.");
                        whileFlag = false;
                    }
                }

                String afterBatchSql = relNode.toString();
                if (!beforeBtachPlan.equals(afterBatchSql)) {
                    logger.info("Result of Batch " + batch.name);
                    logger.info(afterBatchSql);
                } else {
                    logger.trace("Batch " + batch.name + " has no effect.");
                }
            }
        } catch (Exception e) {
            logger.error("Ldb optimizer error");
            return relNode;
        }
        return curRelNode;
    }

}
