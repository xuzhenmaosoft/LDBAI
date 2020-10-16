package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * attribute去重
 */
public class AttributeRexSet implements Serializable {


    Set<NamedRexNode> baseSet;

    public AttributeRexSet(List<NamedRexNode> baseSet) {
        this.baseSet = baseSet.stream().map(ele -> {
            return new AttributeEquals(ele.toAttributeRef());
        }).collect(Collectors.toSet()).stream().map(attributeEqual -> {
            return attributeEqual.attributeRexNode;
        }).collect(Collectors.toSet());
    }

    public AttributeRexSet(Set<NamedRexNode> baseSet) {
        this.baseSet = baseSet;
    }

    /**
     * 对应--方法,先构建Set[AttributeEquals]再参与计算 不能用attributeRexNode,因为不需比较nullable等
     */
    public AttributeRexSet subtraction(List<NamedRexNode> other) {
        Set<AttributeEquals> thisAttributeEqualsSet = baseSet.stream().map(ele -> {
            return new AttributeEquals(ele.toAttributeRef());
        }).collect(Collectors.toSet());

        Set<AttributeEquals> otherAttributeEqualsSet = new AttributeRexSet(other).baseSet.stream()
            .map(ele -> {
                return new AttributeEquals(ele.toAttributeRef());
            }).collect(Collectors.toSet());

        thisAttributeEqualsSet.removeAll(otherAttributeEqualsSet);

        return new AttributeRexSet(thisAttributeEqualsSet.stream().map(attributeEqual -> {
            return attributeEqual.attributeRexNode;
        }).collect(Collectors.toSet()));
    }

    /**
     * 对应++方法
     */
    public AttributeRexSet plusplus(AttributeRexSet other) {
        this.baseSet.addAll(other.baseSet);
        return new AttributeRexSet(this.baseSet);
    }

    public int getSize() {
        return baseSet.size();
    }

    public Set<NamedRexNode> getBaseSet() {
        return baseSet;
    }

    class AttributeEquals {

        AttributeRexNode attributeRexNode;

        AttributeEquals(AttributeRexNode attributeRexNode) {
            this.attributeRexNode = attributeRexNode;
        }

        @Override
        public int hashCode() {
            if (attributeRexNode instanceof StreamRowProcTimeAttributeRexNode) {
                return attributeRexNode.hashCode();
            }
            return attributeRexNode.exprId.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof StreamRowProcTimeAttributeRexNode) {
                return attributeRexNode.equals(obj);
            }
            if (obj instanceof AttributeEquals) {
                return ((AttributeEquals) obj).attributeRexNode.exprId == attributeRexNode.exprId;
            }
            return attributeRexNode == obj;
        }
    }
}
