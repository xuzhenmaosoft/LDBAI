package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import com.codepoetics.protonpack.StreamUtils;
import com.datapps.linkoopdb.worker.spi.plan.core.JoinRelType;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbDistinct;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbExcept;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbJoin;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.Predicate;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.util.Utilities;

public class ReplaceExceptWithAntiJoin extends Rule {

    //TODO assert处理
    @Override
    public RelNode process(RelNode relNode) {
        return relNode.transform((rel) -> {
            if (rel instanceof LdbExcept) {
                RelNode leftRel = (RelNode) rel.getChildren().get(0);
                RelNode rightRel = (RelNode) rel.getChildren().get(1);

                List<RexNode> joinCond = StreamUtils.zip(leftRel.getOutput().stream(), rightRel.getOutput().stream(),
                    (l, r) -> new Predicate(451, ImmutableList.of(l, r))).collect(Collectors.toList());
                LdbJoin ldbJoin = new LdbJoin(leftRel, rightRel, JoinRelType.LEFT_ANTI, Utilities.reduceLeftPredicates(joinCond));
                LdbDistinct ldbDistinct = new LdbDistinct(ldbJoin);
                return ldbDistinct;
            }
            return rel;
        });
    }
}
