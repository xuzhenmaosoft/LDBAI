package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.datapps.linkoopdb.jdbc.types.ArrayType;
import com.datapps.linkoopdb.jdbc.types.RowType;
import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbAggregate;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbExcept;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbFilter;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbGenerate;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbProject;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbUnion;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.AliasRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.LiteralRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.NamedRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.Predicate;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexCall;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;
import com.datapps.linkoopdb.worker.spi.plan.func.SqlAggFunction;
import com.datapps.linkoopdb.worker.spi.plan.util.Utilities;

public class RewriteExceptAll extends Rule {

    @Override
    public RelNode process(RelNode relNode) {
        return relNode.transform((rel) -> {
            if (rel instanceof LdbExcept && ((LdbExcept) rel).all == true) {

                RelNode left = (RelNode) rel.getChildren().get(0);
                RelNode right = (RelNode) rel.getChildren().get(1);

                RexNode newColumnLeft = new AliasRexNode(new LiteralRexNode(1, Type.SQL_BIGINT), "vcol");
                RexNode newColumnRight = new AliasRexNode(new LiteralRexNode(-1, Type.SQL_BIGINT), "vcol");

                LdbProject modifiedLeftPlan = new LdbProject(Utilities.addElementToHead(left.getOutput(), newColumnLeft), left);
                LdbProject modifiedRightPlan = new LdbProject(Utilities.addElementToHead(right.getOutput(), newColumnRight), right);

                LdbUnion unionPlan = new LdbUnion(ImmutableList.of(modifiedLeftPlan, modifiedRightPlan), true);
                AliasRexNode aggSumCol = new AliasRexNode(
                    new SqlAggFunction(SqlKind.SUM, ImmutableList.of(unionPlan.getOutput().get(0)), Type.SQL_INTEGER, false), "sum");

                LdbAggregate aggregatePlan = new LdbAggregate(left.getOutput(),
                    (List<NamedRexNode>) (List) Utilities.addElementToHead(left.getOutput(), (RexNode) aggSumCol), unionPlan);
                LdbFilter filteredAggPlan = new LdbFilter(new Predicate(43,
                    ImmutableList.of(aggSumCol.toAttributeRef(), new LiteralRexNode(0, Type.SQL_INTEGER))), aggregatePlan);

                List<RexNode> replicateRowOutput = Utilities.addElementToHead(left.getOutput(), aggSumCol.toAttributeRef());
                List<Type> typeList = Lists.newArrayList();

                replicateRowOutput.stream().map(attr -> typeList.add(attr.dataType));
                RowType rowType = new RowType(typeList.toArray(new Type[typeList.size()]));

                RexCall replicateRows = new RexCall(SqlKind.REPLICATEROWS, replicateRowOutput, new ArrayType(rowType, replicateRowOutput.size()));
                LdbGenerate genRowPlan = new LdbGenerate(replicateRows, ImmutableList.of(), false, "", left.getOutput(), filteredAggPlan);

                return new LdbProject(left.getOutput(), aggregatePlan);


            }
            return rel;
        });
    }


}
