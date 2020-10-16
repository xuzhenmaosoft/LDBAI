package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.func.WindowSpecDefinition;

/**
 * Created by gloway on 2019/1/30.
 */
public class RexWindow extends RexNode {

    public RexNode windowFunction;
    public WindowSpecDefinition windowSpec;

    public RexWindow(RexNode windowFunction, WindowSpecDefinition windowSpec) {
        this.windowFunction = windowFunction;
        this.windowSpec = windowSpec;
        this.dataType = windowFunction.dataType;
    }

    @Override
    public List<RexNode> getChildren() {
        return ImmutableList.of(windowFunction, windowSpec);
    }

    @Override
    public String simpleString() {
        return windowFunction.simpleString() + " " + windowSpec.simpleString();
    }

    @Override
    public RexWindow withNewChildren(List<RexNode> children) {
        if (!windowFunction.equals(children.get(0)) || !windowSpec.equals(children.get(1))) {
            this.windowFunction = children.get(0);
            this.windowSpec = (WindowSpecDefinition) children.get(1);
            return new RexWindow(this.windowFunction, this.windowSpec);
        }
        return this;
    }
}
