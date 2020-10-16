package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.datapps.linkoopdb.worker.spi.plan.ExprId;

/**
 * Created by gloway on 2019/1/8.
 */
public abstract class NamedRexNode extends RexNode {

    //    private AtomicLong curId = new AtomicLong();
    private static final ThreadLocal<AtomicLong> threadLocal = ThreadLocal.withInitial(AtomicLong::new);
    public String name;
    public ExprId exprId;
    public List<String> qualifier;

    public static UUID jvmId() {
        return UUID.randomUUID();
    }

    public static ExprId newExprId() {
        return new ExprId(threadLocal.get().getAndIncrement(), jvmId());
    }

    public static void resetExprId() {
        threadLocal.get().set(0);
    }
    @Override
    public String simpleString() {
        return name + "#" + exprId.id;
    }

    public abstract AttributeRexNode toAttributeRef();

    public boolean semanticEquals(NamedRexNode other) {
        if (this.exprId.equals(other.exprId)) {
            return true;
        }
        return false;
    }
}
