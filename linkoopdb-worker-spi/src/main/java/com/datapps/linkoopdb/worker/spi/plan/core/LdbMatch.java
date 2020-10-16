package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.queryengine.Transformer;

/**
 * Relational expression that represent a MATCH_RECOGNIZE node.
 *
 * <p>Each output row has the columns defined in the measure statements.
 */
public class LdbMatch extends SingleRel{

    //~ Instance fields ---------------------------------------------
    private static final String STAR = "*";
    protected final List<RexNode> measures;
    protected final RexNode pattern;
    protected final boolean strictStart;
    protected final boolean strictEnd;
    protected final boolean allRows;
    protected final RexNode after;
    protected final ImmutableMap<String, RexNode> patternDefinitions;
    protected final ImmutableMap<String, SortedSet<String>> subsets;
    protected final List<RexNode> partitionKeys;
    protected final LdbSort orderKeys;
    protected final RexNode interval;
    protected final List<RexNode> output;

    //~ Constructors -----------------------------------------------

    public LdbMatch(RelNode input, RexNode pattern, boolean strictStart, boolean strictEnd,
        Map<String, RexNode> patternDefinitions, List<RexNode> measures,
        RexNode after, Map<String, ? extends SortedSet<String>> subsets,
        boolean allRows, List<RexNode> partitionKeys, LdbSort orderKeys,
        RexNode interval, List<RexNode> output) {
        super(input);
        this.pattern = Objects.requireNonNull(pattern);
        Preconditions.checkArgument(patternDefinitions.size() > 0);
        this.strictStart = strictStart;
        this.strictEnd = strictEnd;
        this.patternDefinitions = ImmutableMap.copyOf(patternDefinitions);
        this.measures = measures;
        this.after = Objects.requireNonNull(after);
        this.subsets = copyMap(subsets);
        this.allRows = allRows;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.orderKeys = orderKeys;
        this.interval = interval;
        this.output = Objects.requireNonNull(output);
    }

    /** Creates an immutable map of a map of sorted sets. */
    private static <K extends Comparable<K>, V>
    ImmutableSortedMap<K, SortedSet<V>>
    copyMap(Map<K, ? extends SortedSet<V>> map) {
        final ImmutableSortedMap.Builder<K, SortedSet<V>> b =
            ImmutableSortedMap.naturalOrder();
        for (Map.Entry<K, ? extends SortedSet<V>> e : map.entrySet()) {
            b.put(e.getKey(), ImmutableSortedSet.copyOf(e.getValue()));
        }
        return b.build();
    }

    //~ Methods --------------------------------------------------

    @Override
    public List<RexNode> buildOutput() {
        return this.output;
    }

    @Override
    public RelNode transformExpression(Transformer transformer) {
        return this;
    }

    @Override
    public List<RexNode> expressions() {
        return getOutput();
    }

    @Override
    public RelNode withNewChildren(List<RelNode> children) {
        return this;
    }

    public List<RexNode> getMeasures() {
        return measures;
    }

    public RexNode getPattern() {
        return pattern;
    }

    public boolean isStrictStart() {
        return strictStart;
    }

    public boolean isStrictEnd() {
        return strictEnd;
    }

    public boolean isAllRows() {
        return allRows;
    }

    public RexNode getAfter() {
        return after;
    }

    public ImmutableMap<String, RexNode> getPatternDefinitions() {
        return patternDefinitions;
    }

    public ImmutableMap<String, SortedSet<String>> getSubsets() {
        return subsets;
    }

    public List<RexNode> getPartitionKeys() {
        return partitionKeys;
    }

    public LdbSort getOrderKeys() {
        return orderKeys;
    }

    public RexNode getInterval() {
        return interval;
    }
}
