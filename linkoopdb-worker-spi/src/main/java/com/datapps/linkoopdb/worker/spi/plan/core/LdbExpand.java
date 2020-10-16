package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.codepoetics.protonpack.StreamUtils;
import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.expression.AliasRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.AttributeRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.LiteralRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;
import com.datapps.linkoopdb.worker.spi.plan.util.Utilities;

public class LdbExpand extends SingleRel {

    public List<List<RexNode>> projections;
    public List<RexNode> output;


    public LdbExpand(List<List<RexNode>> projections, List<RexNode> output, RelNode child) {
        super(child);
        this.projections = projections;
        this.output = output;
    }


    public LdbExpand(List<List<AttributeRexNode>> groupingSetsAttrs, List<AliasRexNode> groupByAliases,
        List<AttributeRexNode> groupbyAttrs, AttributeRexNode gid, RelNode child, AttributeRexNode indeId) {
        super(child);

        Map attrMap = StreamUtils.zipWithIndex(groupbyAttrs.stream())
            .collect(Collectors.toMap(index -> index.getValue(), index -> index.getIndex()));

        List<List<RexNode>> projections = groupingSetsAttrs.stream().map(groupingSetAttrs -> {
            List<RexNode> output = child.getOutput();

            // 在output中添加grouping set对应的attr
            output = Utilities.addAll(output, groupbyAttrs.stream().map(attr -> {
                // grouping sets里,不存在的group by attr,输出置为null
                if (!groupingSetAttrs.contains(attr)) {
                    return new LiteralRexNode(null, attr.dataType);
                }
                return attr;
            }).collect(Collectors.toList()));

            output = Utilities.addElementToLast(output, new LiteralRexNode(buildBitMask((List) groupingSetAttrs, attrMap), Type.SQL_INTEGER),
                new LiteralRexNode(genGroupingId(), Type.SQL_INTEGER));
            return output;
        }).collect(Collectors.toList());

        List output = Utilities.addAll(child.getOutput(), groupbyAttrs.stream().map(attr -> {
            return new AttributeRexNode(attr.name, attr.dataType, attr.nullable, NamedRexNode.newExprId(), attr.qualifier);
        }).collect(Collectors.toList()));

        this.projections = projections;
        this.output = Utilities.addElementToLast(output, indeId, gid);
        this.child = new LdbProject(Utilities.addAll(child.getOutput(), (List) groupByAliases), child);
    }

    public String genGroupingId() {
        return UUID.randomUUID().toString();
    }

    /**
     * 在group by中出现,则该位置为0, 未出现为1,主要用于区分null
     * <p>
     * group by attribute a,b,c,d,e
     * grouping set(b,c)
     * = grouping set ((b),(c),())
     * = 10111,11011,11111
     * = 23,27,31
     * <p>
     * group by attribute col1,col2,col3
     * grouping sets((col1,col2),(col2),(col2,col3),(col1,col2,col3))
     * = 001,101,100,111
     * = 1,5,4,0
     *
     * @param groupingSetAttrs The attributes of selected grouping set
     * @param attrMap Mapping group by attributes to its index in attributes sequence
     */
    public int buildBitMask(List<RexNode> groupingSetAttrs, Map<AttributeRexNode, Long> attrMap) {
        int numAttributes = attrMap.size();
        int mask = (1 << numAttributes) - 1;

        // 计算groupingID
        List<Integer> indexList = Utilities.addElementToHead(groupingSetAttrs.stream().filter(attributeRexNode ->
             (attrMap.containsKey(attributeRexNode))
        ).map(attributeRexNode ->
             attrMap.get(attributeRexNode)
        ).map(index ->
              ~(1 << (numAttributes - 1 - index))
        ).collect(Collectors.toList()), mask);

        return indexList.stream().reduce((e1, e2) ->
             e1 & e2
        ).orElse(-1);

    }


    @Override
    public List<RexNode> buildOutput() {
        return output;
    }

    @Override
    public RelNode transformExpression(Transformer transformer) {
        List<List<RexNode>> projectionsAfterRule = projections.stream().map(list ->
            transformExpression(list, transformer)).collect(Collectors.toList());
        List<RexNode> outputAfterRule = transformExpression(output, transformer);
        if (!projections.equals(projectionsAfterRule) || !output.equals(outputAfterRule)) {
            this.projections = projectionsAfterRule;
            this.output = outputAfterRule;

            return new LdbExpand(this.projections, this.output, this.child);
        }
        return this;
    }

    @Override
    public List<RexNode> expressions() {
        return getOutput();
    }

    @Override
    public LdbExpand withNewChildren(List<RelNode> children) {
        if (!child.fastEquals(children.get(0))) {
            child = children.get(0);
            return new LdbExpand(projections, output, child);
        }
        return this;
    }

}
