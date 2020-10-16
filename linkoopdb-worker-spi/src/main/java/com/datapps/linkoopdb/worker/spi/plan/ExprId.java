package com.datapps.linkoopdb.worker.spi.plan;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;

/**
 * Created by gloway on 2019/1/8.
 */
public class ExprId implements Serializable {

    public final Long id;
    public final UUID jvmId;

    public ExprId(Long id) {
        this.id = id;
        jvmId = NamedRexNode.jvmId();
    }

    public ExprId(Long id, UUID jvmId) {
        this.id = id;
        this.jvmId = jvmId;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ExprId) {
            ExprId otherExp = (ExprId) other;
            return Objects.equals(id, otherExp.id) && jvmId.equals(otherExp.jvmId);
        } else {
            return false;
        }
    }
}
