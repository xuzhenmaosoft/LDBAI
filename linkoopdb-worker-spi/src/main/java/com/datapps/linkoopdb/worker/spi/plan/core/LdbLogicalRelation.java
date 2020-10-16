package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.expression.AttributeRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Created by gloway on 2019/1/14.
 */
public class LdbLogicalRelation extends RelNode {

    private List<AttributeRexNode> outputAttr;

    private LdbSource source;

    private String indexMetastorePath;

    public LdbLogicalRelation(LdbSource source, List<AttributeRexNode> attributeReferences) {
        this.source = source;
        this.outputAttr = attributeReferences;
    }

    public LdbLogicalRelation(LdbSource source, List<AttributeRexNode> attributeReferences, String indexMetastorePath) {
        this.source = source;
        this.outputAttr = attributeReferences;
        this.indexMetastorePath = indexMetastorePath;
    }

    @Override
    public List<RexNode> buildOutput() {
        return ImmutableList.copyOf(outputAttr);
    }

    @Override
    public List<RexNode> expressions() {
        return ImmutableList.of();
    }

    public LdbSource getSource() {
        return source;
    }

    public void setSource(LdbSource source) {
        this.source = source;
    }

    @Override
    public LdbLogicalRelation transformExpression(Transformer transformer) {
        return this;
    }

    @Override
    public LdbLogicalRelation withNewChildren(List<RelNode> children) {
        return this;
    }

    public void setOutputAttr(List<AttributeRexNode> outputAttr) {
        this.outputAttr = outputAttr;
    }
    public void updateOutput() {
        this.output = buildOutput();
    }

    public void setIndexMetastorePath(String indexMetastorePath) {
        this.indexMetastorePath = indexMetastorePath;
    }

    public String getIndexMetastorePath() {
        return indexMetastorePath;
    }
}
