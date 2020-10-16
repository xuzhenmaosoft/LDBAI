package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import com.codepoetics.protonpack.StreamUtils;
import com.datapps.linkoopdb.worker.spi.plan.core.JoinRelType;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbDistinct;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbIntersect;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbJoin;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.Predicate;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.util.Utilities;

public class ReplaceIntersectWithSemiJoin extends Rule {

    @Override
    public RelNode process(RelNode relNode) {
        return relNode.transform((rel) -> {
            if (rel instanceof LdbIntersect && ((LdbIntersect) rel).all == false) {
                RelNode leftRel = (RelNode) rel.getChildren().get(0);
                RelNode rightRel = (RelNode) rel.getChildren().get(1);

                List<RexNode> joinCond = StreamUtils.zip(leftRel.getOutput().stream(), rightRel.getOutput().stream(),
                    (l, r) -> new Predicate(451, ImmutableList.of(l, r))).collect(Collectors.toList());
                LdbJoin join = new LdbJoin(leftRel, rightRel, JoinRelType.LEFT_SEMI, Utilities.reduceLeftPredicates(joinCond));
                LdbDistinct distinct = new LdbDistinct(join);
                return distinct;
            }
            return rel;
        });
    }
}
