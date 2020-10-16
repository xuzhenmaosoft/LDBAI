package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.datapps.linkoopdb.jdbc.types.ArrayType;
import com.datapps.linkoopdb.jdbc.types.RowType;
import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbAggregate;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbFilter;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbGenerate;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbIntersect;
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

public class RewriteIntersectAll extends Rule {

    @Override
    public RelNode process(RelNode relNode) {
        return relNode.transform((rel) -> {
            if (rel instanceof LdbIntersect && ((LdbIntersect) rel).all == true) {

                RelNode left = (RelNode) rel.getChildren().get(0);
                RelNode right = (RelNode) rel.getChildren().get(1);

                RexNode trueVcol1 = new AliasRexNode(new LiteralRexNode(true, Type.SQL_BOOLEAN), "vcol1");
                RexNode nullVcol1 = new AliasRexNode(new LiteralRexNode(null, Type.SQL_BOOLEAN), "vcol1");
                RexNode trueVcol2 = new AliasRexNode(new LiteralRexNode(true, Type.SQL_BOOLEAN), "vcol2");
                RexNode nullVcol2 = new AliasRexNode(new LiteralRexNode(null, Type.SQL_BOOLEAN), "vcol2");

                LdbProject leftPlanWithAddedVirtualCols = new LdbProject(Utilities.addElementToHead(left.getOutput(), trueVcol1, nullVcol2), left);
                LdbProject rightPlanWithAddedVirtualCols = new LdbProject(Utilities.addElementToHead(right.getOutput(), nullVcol1, trueVcol2), right);

                LdbUnion unionPlan = new LdbUnion(ImmutableList.of(leftPlanWithAddedVirtualCols, rightPlanWithAddedVirtualCols), true);

                RexNode rexNode1 = unionPlan.getOutput().get(0);
                RexNode rexNode2 = unionPlan.getOutput().get(1);

                AliasRexNode vCol1AggrExpr = new AliasRexNode(new SqlAggFunction(SqlKind.COUNT,
                    ImmutableList.of(rexNode1),
                    Type.SQL_BIGINT, false), "vcol1_count");
                AliasRexNode vCol2AggrExpr = new AliasRexNode(new SqlAggFunction(SqlKind.COUNT,
                    ImmutableList.of(rexNode2),
                    Type.SQL_BIGINT, false), "vcol2_count");

                Predicate greaterThan = new Predicate(43, ImmutableList.of(vCol1AggrExpr.toAttributeRef(), vCol2AggrExpr.toAttributeRef()));

                RexCall ifRexCall = new RexCall(SqlKind.IF, ImmutableList.of(greaterThan, vCol1AggrExpr.toAttributeRef(), vCol2AggrExpr.toAttributeRef()),
                    Type.SQL_BIGINT);

                AliasRexNode ifExpression = new AliasRexNode(ifRexCall, "min_count");

                LdbAggregate aggregatePlan = new LdbAggregate(left.getOutput(),
                    (List<NamedRexNode>) (List) Utilities.addElementToHead(left.getOutput(), vCol1AggrExpr, vCol2AggrExpr), unionPlan);

                Predicate GreaterThanOrEqual1 = new Predicate(41,
                    ImmutableList.of(vCol1AggrExpr.toAttributeRef(), new LiteralRexNode(1, Type.SQL_INTEGER)));

                Predicate GreaterThanOrEqual2 = new Predicate(41,
                    ImmutableList.of(vCol2AggrExpr.toAttributeRef(), new LiteralRexNode(1, Type.SQL_INTEGER)));

                LdbFilter filterPlan = new LdbFilter(new Predicate(49, ImmutableList.of(GreaterThanOrEqual1, GreaterThanOrEqual2)), aggregatePlan);

                LdbProject projectMinPlan = new LdbProject(Utilities.addElementToLast(left.getOutput(), ifExpression), filterPlan);

                List<RexNode> replicateRowOutput = Utilities.addElementToHead(left.getOutput(), ifExpression.toAttributeRef());
                List<Type> types = Lists.newArrayList();

                replicateRowOutput.stream().map(attr -> types.add(attr.dataType));
                RowType rowType = new RowType(types.toArray(new Type[types.size()]));

                RexCall replicateRows = new RexCall(SqlKind.REPLICATEROWS, replicateRowOutput, new ArrayType(rowType, replicateRowOutput.size()));
                LdbGenerate genRowPlan = new LdbGenerate(replicateRows, ImmutableList.of(), false, "", left.getOutput(), projectMinPlan);

                return new LdbProject(left.getOutput(), genRowPlan);
                //  return  aggregatePlan;

            }
            return rel;
        });
    }
}
