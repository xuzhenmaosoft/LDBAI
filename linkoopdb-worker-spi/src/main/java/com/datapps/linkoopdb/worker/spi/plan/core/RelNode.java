package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.LdbStatistics;
import com.datapps.linkoopdb.worker.spi.plan.TreeNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.AttributeRexSet;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SubqueryRexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/7.
 */
public abstract class RelNode extends TreeNode<RelNode> {

    public List<RexNode> output;
    private LdbStatistics statsCache;

    @Override
    public List<RelNode> getChildren() {
        return Collections.emptyList();
    }

    public final List<RexNode> getOutput() {
        if (output == null) {
            output = buildOutput();
        }
        return output;
    }

    public abstract List<RexNode> buildOutput();

    public abstract RelNode transformExpression(Transformer transformer);

    @Override
    public List<RelNode> getInnerChildren() {
        ArrayList<RelNode> subQuery = new ArrayList<>();

        for (RexNode rexNode : expressions()) {
            rexNode.foreach(rel -> {
                if (rel instanceof SubqueryRexNode) {
                    subQuery.add(((SubqueryRexNode) rel).plan);
                }
            });
        }
        return subQuery;
    }

    public abstract List<RexNode> expressions();

    @Override
    public String simpleString() {
        if (statsCache == null) {
            return super.simpleString();
        } else {
            return super.simpleString() + ", Statistics(" + statsCache + ')';
        }
    }

    @Override
    protected String argString() {
        return mkString(getOutput());
    }

    public List<RexNode> transformExpression(List<RexNode> rexNodeList, Transformer transformer) {
        List<RexNode> newList = new ArrayList();
        for (RexNode rexNode : rexNodeList) {
            RexNode afterRule = rexNode.transform(transformer);
            if (rexNode.equals(afterRule)) {
                newList.add(rexNode);
            } else {
                newList.add(afterRule);
            }
        }
        return ImmutableList.copyOf(newList);
    }

    public List<RexNode> transformExpressionUp(List<RexNode> rexNodeList, Transformer transformer) {
        List<RexNode> newList = new ArrayList();
        for (RexNode rexNode : rexNodeList) {
            RexNode afterRule = rexNode.transformUp(transformer);
            if (rexNode.equals(afterRule)) {
                newList.add(rexNode);
            } else {
                newList.add(afterRule);
            }
        }
        return ImmutableList.copyOf(newList);
    }


    public AttributeRexSet references() {
        return new AttributeRexSet(expressions().stream().flatMap(child ->
            child.references().getBaseSet().stream()
        ).collect(Collectors.toSet()));
    }

    public LdbStatistics getStats() {
        if (statsCache == null) {
            // TODO: 增加stats收集的任务
        }
        return statsCache;
    }

    public void setStats(LdbStatistics stats) {
        statsCache = stats;
    }

}

