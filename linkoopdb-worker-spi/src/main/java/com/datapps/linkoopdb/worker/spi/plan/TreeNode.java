package com.datapps.linkoopdb.worker.spi.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;
import com.datapps.linkoopdb.worker.spi.plan.util.Utilities;

/**
 * Created by gloway on 2019/1/7.
 */
public abstract class TreeNode<BaseType extends TreeNode<BaseType>> implements Serializable {

    public String nodeName = getClass().getSimpleName();

    public static String mkString(List<? extends TreeNode> children) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        for (int i = 0; i < children.size(); i++) {
            stringBuilder.append(children.get(i).simpleString());
            if (i != children.size() - 1) {
                stringBuilder.append(',').append(' ');
            }
        }
        return stringBuilder.append(']').toString();
    }

    public boolean fastEquals(TreeNode other) {
        return this.equals(other) || this == other;
    }

    public abstract List<BaseType> getChildren();

    public abstract BaseType withNewChildren(List<BaseType> children);

    /*public BaseType withNewChildren(List<BaseType> newChildren){
        assert(newChildren.size() == children.size());
        boolean changed = false;
        retu

    }*/

    public List<BaseType> getInnerChildren() {
        return ImmutableList.of();
    }

    public StringBuilder generateTreeString(int depth, List<Boolean> lastChildren, StringBuilder builder) {
        if (depth > 0) {
            for (int i = 0; i < lastChildren.size(); i++) {
                boolean isLast = lastChildren.get(i);
                if (i == lastChildren.size() - 1) {
                    if (isLast) {
                        builder.append("+- ");
                    } else {
                        builder.append(":- ");
                    }
                    break;
                }
                if (isLast) {
                    builder.append("   ");
                } else {
                    builder.append(":  ");
                }
            }
        }

        builder.append(simpleString()).append('\n');

        List<BaseType> children = getChildren();

        List<BaseType> innerChildren = getInnerChildren();

        if (!innerChildren.isEmpty()) {
            for (int i = 0; i < innerChildren.size() - 1; i++) {
                ArrayList<Boolean> childrenCopy = new ArrayList<>(Arrays.asList(new Boolean[lastChildren.size()]));
                Collections.copy(childrenCopy, lastChildren);
                childrenCopy.add(children.isEmpty());
                childrenCopy.add(false);
                innerChildren.get(i).generateTreeString(depth + 2, childrenCopy, builder);
            }

            ArrayList<Boolean> childrenCopy = new ArrayList<>(Arrays.asList(new Boolean[lastChildren.size()]));
            Collections.copy(childrenCopy, lastChildren);
            childrenCopy.add(children.isEmpty());
            childrenCopy.add(true);
            innerChildren.get(innerChildren.size() - 1).generateTreeString(depth + 2, childrenCopy, builder);
        }

        if (!children.isEmpty()) {
            for (int i = 0; i < children.size() - 1; i++) {
                ArrayList<Boolean> childrenCopy = new ArrayList<>(Arrays.asList(new Boolean[lastChildren.size()]));
                Collections.copy(childrenCopy, lastChildren);
                childrenCopy.add(false);
                children.get(i).generateTreeString(depth + 1, childrenCopy, builder);
            }

            ArrayList<Boolean> childrenCopy = new ArrayList<>(Arrays.asList(new Boolean[lastChildren.size()]));
            Collections.copy(childrenCopy, lastChildren);
            childrenCopy.add(true);
            children.get(children.size() - 1).generateTreeString(depth + 1, childrenCopy, builder);
        }

        return builder;
    }

    public String toString() {
        return generateTreeString(0, ImmutableList.of(), new StringBuilder()).toString();
    }

    public String simpleString() {
        return nodeName + " " + argString();
    }

    protected abstract String argString();

    public void foreach(ForeachInter foreachInter) {
        foreachInter.transform(this);
        getChildren().forEach(x -> x.foreach(foreachInter));
    }


    public BaseType transform(Transformer transformer) {
        return transformDown(transformer);
    }

    public BaseType transformUp(Transformer transformer) {
        List childList = new ArrayList();
        getChildren().forEach(child -> {
            BaseType afterRule = (BaseType) child.transformUp(transformer);
            childList.add(afterRule);
        });
        BaseType newBaseType = (BaseType) this.withNewChildren(childList);
        return (BaseType) transformer.execute(newBaseType);
    }


    public BaseType transformDown(Transformer transformer) {
        BaseType curNode;
        BaseType afterRule = (BaseType) transformer.execute(this);

        if (equals(afterRule)) {
            curNode = (BaseType) this;
        } else {
            curNode = afterRule;
        }
        List childList = new ArrayList();
        curNode.getChildren().forEach(child -> {
                childList.add(child.transformDown(transformer));
            }
        );
        BaseType newBaseType = (BaseType) afterRule.withNewChildren(ImmutableList.copyOf(childList));
        return newBaseType;
    }

    public BaseType find(FindInter findInter) {
        if (findInter.transform(this)) {
            return (BaseType) this;
        } else {
            List<BaseType> children = getChildren();
            for (BaseType c : children) {
                BaseType res = c.find(findInter);
                if (res != null) {
                    return res;
                }
            }
            return null;
        }
    }

    public List collect(CollectFunction collectFunction) {
        List ret = Lists.newArrayList();
        foreach(rel -> {
            Optional result = collectFunction.transform(rel);
            result.map(item -> ret.add(result.get()));
        });
        return ret;
    }


    public Optional<BaseType> collectFirst(CollectFunction collectFunction) {
        Optional<BaseType> optional = Optional.empty();
        Optional<BaseType> res = collectFunction.transform(this);

        if (!res.isPresent()) {
            return Utilities.foldLeft(getChildren(), optional, (l, r) -> {
                if (!l.isPresent()) {
                    return r.collectFirst(collectFunction);
                }
                return l;
            });
        }
        return res;
    }
}
