package com.datapps.linkoopdb.worker.spi.plan.queryengine.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import com.datapps.linkoopdb.worker.spi.plan.core.LdbPipeline;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbPipelineStage;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbTableFunctionWrapper;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.AliasRexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexCall;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;

/**
 * When using pipeline for image data transformation some function can be combined to single stage for saving memory and computing resource. For example : Using
 * functions 'resize' and 'cvtcolor' as pipeline stages can be combined to single stage
 *
 * @author xingbu
 * @version 1.0
 *
 * created by　19-7-31 5:11 pm
 */
public class CombineImageFunctions extends Rule {

    private static Set<SqlKind> optimizedFunctions = new HashSet<>();

    enum ImageFunctionRule {
        // todo support more
        COMBINE_RESIZE_CVTCOLOR(false, SqlKind.RESIZE, SqlKind.CVTCOLOR);

        boolean orderDepended;
        Collection<SqlKind> functionCombination;

        ImageFunctionRule(boolean orderDepended, SqlKind... functionCombination) {
            this.orderDepended = orderDepended;
            if (orderDepended) {
                this.functionCombination = Arrays.asList(functionCombination);
            } else {
                this.functionCombination = new HashSet<>(Arrays.asList(functionCombination));
            }
            optimizedFunctions.addAll(Arrays.asList(functionCombination));
        }

        boolean check(SqlKind[] functions) {
            if (!orderDepended) {
                for (int i = 0; i < functions.length; i++) {
                    if (!optimizedFunctions.contains(functions[i])) {
                        continue;
                    }
                    Set<SqlKind> functionSet = new HashSet<>(functionCombination);
                    int count = 0;
                    for (int j = i; j < functions.length; j++) {
                        if (functionSet.contains(functions[j])) {
                            functionSet.remove(functions[j]);
                            count++;
                            if (count == functionCombination.size()) {
                                return true;
                            }
                        } else {
                            break;
                        }
                    }

                }
                return false;
            } else {
                for (int i = 0; i < functions.length; i++) {
                    if (!optimizedFunctions.contains(functions[i])) {
                        continue;
                    }
                    int k = i;
                    for (int j = 0; j < functionCombination.size(); j++) {
                        if (!((List) functionCombination).get(j).equals(functions[k])) {
                            break;
                        } else {
                            if (j == functionCombination.size() - 1) {
                                return true;
                            }
                            k++;
                        }
                    }
                }
                return false;
            }
        }

        static List<ImageFunctionRule> extractRulesForFunctions(SqlKind[] functions) {
            ImageFunctionRule[] rules = ImageFunctionRule.values();
            List<ImageFunctionRule> imageFunctionRules = new ArrayList<>();
            for (int i = 0; i < rules.length; i++) {
                ImageFunctionRule imageFunctionRule = rules[i];
                if (imageFunctionRule.check(functions)) {
                    imageFunctionRules.add(imageFunctionRule);
                    functions = remove(imageFunctionRule, functions);
                }
            }
            return imageFunctionRules;
        }

        private static SqlKind[] remove(ImageFunctionRule imageFunctionRule, SqlKind[] functions) {
            Collection<SqlKind> functionCombination = imageFunctionRule.functionCombination;
            boolean[] removeFlag = new boolean[functions.length];
            for (SqlKind functionComponent : functionCombination) {
                for (int j = 0; j < functions.length; j++) {
                    SqlKind function = functions[j];
                    if (function.equals(functionComponent)) {
                        removeFlag[j] = true;
                        break;
                    }
                }
            }
            List<SqlKind> remains = new ArrayList<>();
            for (int i = 0; i < functions.length; i++) {
                if (!removeFlag[i]) {
                    remains.add(functions[i]);
                }
            }
            return remains.toArray(new SqlKind[0]);
        }
    }

    @Override
    public RelNode process(RelNode relNode) {
        if (!(relNode instanceof LdbPipeline)) {
            return relNode;
        }
        List<SqlKind> functions = extractFunctions((LdbPipeline) relNode);
        List<ImageFunctionRule> imageFunctionRules = ImageFunctionRule.extractRulesForFunctions(functions.toArray(new SqlKind[0]));
        Collections.sort(imageFunctionRules);
        LdbPipeline pipeline = (LdbPipeline) relNode;
        for (ImageFunctionRule imageFunctionRule : imageFunctionRules) {
            pipeline = process(imageFunctionRule, pipeline);
        }
        return pipeline;
    }

    private LdbPipeline process(ImageFunctionRule imageFunctionRule, LdbPipeline pipeline) {
        switch (imageFunctionRule) {
            case COMBINE_RESIZE_CVTCOLOR:
                return processResizeAndCvtcolor(pipeline);
            default:
                return pipeline;
        }
    }


    private LdbPipeline processResizeAndCvtcolor(LdbPipeline pipeline) {
        List<LdbPipelineStage> stages = pipeline.getStages();
        List<LdbPipelineStage> newStages = new ArrayList<>();
        LdbPipeline ldbPipeline = new LdbPipeline(pipeline.getType(), pipeline.getSource(), newStages);
        boolean done = false;
        for (int i = 0; i < stages.size(); i++) {
            LdbPipelineStage ldbPipelineStage = stages.get(i);
            RexCall functionCall = getFunctionCall(ldbPipelineStage);
            if ((SqlKind.RESIZE.equals(functionCall.sqlKind) || SqlKind.CVTCOLOR.equals(functionCall.sqlKind)) && !done) {
                LdbPipelineStage nextLdbPipelineStage = stages.get(i + 1);
                RexCall nextFunction = getFunctionCall(nextLdbPipelineStage);
                LdbPipelineStage combinedPipelineStage = new LdbPipelineStage(nextLdbPipelineStage.getType(),
                    ldbPipelineStage.getStepName() + "_" + nextLdbPipelineStage.getStepName());
                LdbTableFunctionWrapper ldbTableFunctionWrapper = new LdbTableFunctionWrapper(nextFunction, ldbPipelineStage.getInputCols(),
                    nextLdbPipelineStage.getOutputCols());
                combinedPipelineStage.setStageEntity(ldbTableFunctionWrapper);
                List<RexNode> operands = nextFunction.operands;
                List<RexNode> rexNodeArrayList = new ArrayList<>();
                rexNodeArrayList.add(functionCall);
                for (int j = 1; j < operands.size(); j++) {
                    rexNodeArrayList.add(operands.get(j));
                }
                i++;
                done = true;
                nextFunction.operands = ImmutableList.copyOf(rexNodeArrayList);
                newStages.add(combinedPipelineStage);
            } else {
                newStages.add(ldbPipelineStage);
            }
        }

        return ldbPipeline;
    }

    private RexCall getFunctionCall(LdbPipelineStage ldbPipelineStage) {
        RexNode functionRexNode = ldbPipelineStage.getFunctionRexNode();
        RexCall functionCall;
        functionCall = getRexCall(functionRexNode);
        return functionCall;
    }

    private RexCall getRexCall(RexNode functionRexNode) {
        RexCall functionCall;
        if (functionRexNode instanceof AliasRexNode) {
            AliasRexNode aliasRexNode = (AliasRexNode) functionRexNode;
            functionCall = (RexCall) aliasRexNode.child;
        } else {
            functionCall = (RexCall) functionRexNode;
        }
        return functionCall;
    }

    private List<SqlKind> extractFunctions(LdbPipeline pipeline) {
        List<LdbPipelineStage> stages = pipeline.getStages();
        List<SqlKind> functions = new ArrayList<>();
        for (int i = 0; i < stages.size(); i++) {
            LdbPipelineStage ldbPipelineStage = stages.get(i);
            RexNode functionRexNode = ldbPipelineStage.getFunctionRexNode();
            if (functionRexNode instanceof AliasRexNode) {
                AliasRexNode aliasRexNode = (AliasRexNode) functionRexNode;
                RexNode child = aliasRexNode.child;
                if (child instanceof RexCall) {
                    RexCall function = (RexCall) child;
                    functions.add(function.sqlKind);
                }
            } else {
                RexCall rexCall = (RexCall) functionRexNode;
                functions.add(rexCall.sqlKind);
            }
        }
        return functions;
    }

}
