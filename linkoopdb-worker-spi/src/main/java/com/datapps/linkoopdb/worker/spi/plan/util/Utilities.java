/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datapps.linkoopdb.worker.spi.plan.util;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.datapps.linkoopdb.worker.spi.plan.expression.Predicate;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexOuterRef;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Utility methods
 */
public class Utilities {

    // Even though this is a utility class (all methods are static), we cannot
    // make the constructor private. Because Janino doesn't do static import,
    // generated code is placed in sub-classes.
    protected Utilities() {
    }

    /**
     * @deprecated Use {@link Objects#equals}.
     */
    @Deprecated // to be removed before 2.0
    public static boolean equal(Object o0, Object o1) {
        // Same as java.lang.Objects.equals (JDK 1.7 and later)
        // and com.google.common.base.Objects.equal
        return Objects.equals(o0, o1);
    }

    public static int hash(Object v) {
        return v == null ? 0 : v.hashCode();
    }

    /**
     * Computes the hash code of a {@code double} value. Equivalent to
     * {@link Double}{@code .hashCode(double)}, but that method was only
     * introduced in JDK 1.8.
     *
     * @param v Value
     * @return Hash code
     * @deprecated Use {@link Double#hashCode(double)}
     */
    @Deprecated // to be removed before 2.0
    public static int hashCode(double v) {
        return Double.hashCode(v);
    }

    /**
     * Computes the hash code of a {@code float} value. Equivalent to
     * {@link Float}{@code .hashCode(float)}, but that method was only
     * introduced in JDK 1.8.
     *
     * @param v Value
     * @return Hash code
     * @deprecated Use {@link Float#hashCode(float)}
     */
    @Deprecated // to be removed before 2.0
    public static int hashCode(float v) {
        return Float.hashCode(v);
    }

    /**
     * Computes the hash code of a {@code long} value. Equivalent to
     * {@link Long}{@code .hashCode(long)}, but that method was only
     * introduced in JDK 1.8.
     *
     * @param v Value
     * @return Hash code
     * @deprecated Use {@link Long#hashCode(long)}
     */
    @Deprecated // to be removed before 2.0
    public static int hashCode(long v) {
        return Long.hashCode(v);
    }

    /**
     * Computes the hash code of a {@code boolean} value. Equivalent to
     * {@link Boolean}{@code .hashCode(boolean)}, but that method was only
     * introduced in JDK 1.8.
     *
     * @param v Value
     * @return Hash code
     * @deprecated Use {@link Boolean#hashCode(boolean)}
     */
    @Deprecated // to be removed before 2.0
    public static int hashCode(boolean v) {
        return Boolean.hashCode(v);
    }

    public static int hash(int h, boolean v) {
        return h * 31 + hashCode(v);
    }

    public static int hash(int h, byte v) {
        return h * 31 + v;
    }

    public static int hash(int h, char v) {
        return h * 31 + v;
    }

    public static int hash(int h, short v) {
        return h * 31 + v;
    }

    public static int hash(int h, int v) {
        return h * 31 + v;
    }

    public static int hash(int h, long v) {
        return h * 31 + Long.hashCode(v);
    }

    public static int hash(int h, float v) {
        return hash(h, Float.hashCode(v));
    }

    public static int hash(int h, double v) {
        return hash(h, Double.hashCode(v));
    }

    public static int hash(int h, Object v) {
        return h * 31 + (v == null ? 1 : v.hashCode());
    }

    public static int compare(boolean v0, boolean v1) {
        return Boolean.compare(v0, v1);
    }

    public static int compare(byte v0, byte v1) {
        return Byte.compare(v0, v1);
    }

    public static int compare(char v0, char v1) {
        return Character.compare(v0, v1);
    }

    public static int compare(short v0, short v1) {
        return Short.compare(v0, v1);
    }

    public static int compare(int v0, int v1) {
        return Integer.compare(v0, v1);
    }

    public static int compare(long v0, long v1) {
        return Long.compare(v0, v1);
    }

    public static int compare(float v0, float v1) {
        return Float.compare(v0, v1);
    }

    public static int compare(double v0, double v1) {
        return Double.compare(v0, v1);
    }

    public static int compare(List v0, List v1) {
        //noinspection unchecked
        final Iterator iterator0 = v0.iterator();
        final Iterator iterator1 = v1.iterator();
        for (; ; ) {
            if (!iterator0.hasNext()) {
                return !iterator1.hasNext()
                    ? 0
                    : -1;
            }
            if (!iterator1.hasNext()) {
                return 1;
            }
            final Object o0 = iterator0.next();
            final Object o1 = iterator1.next();
            int c = compare_(o0, o1);
            if (c != 0) {
                return c;
            }
        }
    }

    private static int compare_(Object o0, Object o1) {
        if (o0 instanceof Comparable) {
            return compare((Comparable) o0, (Comparable) o1);
        }
        return compare((List) o0, (List) o1);
    }

    public static int compare(Comparable v0, Comparable v1) {
        //noinspection unchecked
        return v0.compareTo(v1);
    }

    public static int compareNullsFirst(Comparable v0, Comparable v1) {
        //noinspection unchecked
        return v0 == v1 ? 0
            : v0 == null ? -1
                : v1 == null ? 1
                    : v0.compareTo(v1);
    }

    public static int compareNullsLast(Comparable v0, Comparable v1) {
        //noinspection unchecked
        return v0 == v1 ? 0
            : v0 == null ? 1
                : v1 == null ? -1
                    : v0.compareTo(v1);
    }

    public static int compareNullsLast(List v0, List v1) {
        //noinspection unchecked
        return v0 == v1 ? 0
            : v0 == null ? 1
                : v1 == null ? -1
                    : FlatLists.ComparableListImpl.compare(v0, v1);
    }

    public static String bytesToString(long size) {
        long EB = 1L << 60;
        long PB = 1L << 50;
        long TB = 1L << 40;
        long GB = 1L << 30;
        long MB = 1L << 20;
        long KB = 1L << 10;

        if (size >= (1L << 11) * EB) {
            // The number is too large, show it in scientific notation.
            return new BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B";
        } else {
            BigDecimal value;
            String unit;
            if (size >= 2 * EB) {
                value = (new BigDecimal(size)).divide(new BigDecimal(EB));
                unit = "EB";
            } else if (size >= 2 * PB) {
                value = (new BigDecimal(size)).divide(new BigDecimal(EB));
                unit = "PB";
            } else if (size >= 2 * TB) {
                value = (new BigDecimal(size)).divide(new BigDecimal(EB));
                unit = "TB";
            } else if (size >= 2 * GB) {
                value = (new BigDecimal(size)).divide(new BigDecimal(EB));
                unit = "GB";
            } else if (size >= 2 * MB) {
                value = (new BigDecimal(size)).divide(new BigDecimal(EB));
                unit = "MB";
            } else if (size >= 2 * KB) {
                value = (new BigDecimal(size)).divide(new BigDecimal(EB));
                unit = "KB";
            } else {
                value = (new BigDecimal(size));
                unit = "B";
            }
            return value + " " + unit;
        }
    }


    /**
     * 集合转秩
     */
    public static <T> List<List<T>> transpose(List<List<T>> lists) {
        List result = new ArrayList();
        if (lists.isEmpty()) {
            return result;
        }
        int minSize = Integer.MAX_VALUE;
        for (List<T> listLike : lists) {
            if (listLike.size() < minSize) {
                minSize = listLike.size();
            }
        }
        if (minSize == 0) {
            return result;
        }
        for (int i = 0; i < minSize; i++) {
            result.add(new ArrayList());
        }
        for (List<T> listLike : lists) {
            for (int i = 0; i < minSize; i++) {
                List resultList = (List) result.get(i);
                resultList.add(listLike.get(i));
            }
        }
        return result;
    }

    public static List<RexNode> stripOuterReferences(List<RexNode> e) {
        return e.stream().map(ele -> stripOuterReference(ele)).collect(Collectors.toList());
    }

    public static RexNode stripOuterReference(RexNode rexNode) {
        return rexNode.transform(rex -> {
            if (rex instanceof RexOuterRef) {
                return ((RexOuterRef) rex).e;
            }
            return rex;
        });
    }


    /**
     * 将rexNode按照AND切分,放到list中
     */
    public static void splitConjunctivePredicates(RexNode rexNode, List<RexNode> candidates) {
        if (rexNode instanceof Predicate && ((Predicate) rexNode).sqlKind == SqlKind.AND) {
            splitConjunctivePredicates(((Predicate) rexNode).operands.get(0), candidates);
            splitConjunctivePredicates(((Predicate) rexNode).operands.get(1), candidates);
        } else {
            candidates.add(rexNode);
        }
    }


    /**
     * 将list中的rexNode,按照AND进行合并
     */
    public static RexNode reduceLeftPredicates(List<RexNode> candidates) {
        if (candidates.isEmpty()) {
            return null;
            //throw new IllegalArgumentException("reduce require to have at least one element in the collection");
        }
        RexNode acc = candidates.get(0);
        int i = 0;
        int size = candidates.size();
        while (i < size - 1) {
            i += 1;
            acc = new Predicate(49, ImmutableList.of(acc, candidates.get(i)));
        }
        return acc;
    }

    public static <T> List<T> addElementToLast(List<T> list, T... t) {
        List newListA = Lists.newArrayList(list);
        List newListB = Lists.newArrayList(t);

        newListA.addAll(newListB);

        if (list instanceof ImmutableList) {
            return ImmutableList.copyOf(newListA);
        }

        return newListA;
    }

    /**
     * 加元素添加到集合头部
     */
    public static <T> List<T> addElementToHead(List<T> list, T... t) {
        List newListA = Lists.newArrayList(list);
        List newListB = Lists.newArrayList(t);

        newListB.addAll(newListA);

        if (list instanceof ImmutableList) {
            return ImmutableList.copyOf(newListB);
        }

        return newListB;
    }

    /**
     * 合并两个集合
     */
    public static <T> List<T> addAll(List<T> list, List<T> listRight) {
        List newListA = Lists.newArrayList(list);
        List newListB = Lists.newArrayList(listRight);

        newListA.addAll(newListB);

        if (list instanceof ImmutableList) {
            return ImmutableList.copyOf(newListA);
        }

        return newListA;
    }

    /**
     * 将List集合根据booleanFunc分为满足条件的子集合,和不满足条件的子集合
     */
    public static <T> Pair<List<T>, List<T>> partition(List<T> elements, Utilities.BooleanFunc<T> booleanFunc) {
        List<T> trueLists = Lists.newArrayList();
        List<T> falseLists = Lists.newArrayList();
        elements.forEach(element -> {
            if (booleanFunc.estimate(element)) {
                trueLists.add(element);
            } else {
                falseLists.add(element);
            }
        });
        return Pair.of(trueLists, falseLists);
    }

    /**
     * 从左向右累加
     *
     * @param list 初始集合
     * @param initValue 初始值
     * @param accFunc 累加函数
     * @param <T> 结果类型
     * @param <R> 集合中元素类型
     */
    public static <T, R> T foldLeft(List<R> list, T initValue, Utilities.AccFunc<T, R> accFunc) {
        T result = initValue;
        for (R obj : list) {
            result = accFunc.acc(result, obj);
        }
        return result;
    }

    /**
     * 在list中找到满足条件的元素的位置
     *
     * @param list 原始集合
     * @param booleanFunc 条件函数
     * @param <T> 集合类型
     */
    public static <T> int indexWhere(List<T> list, Utilities.BooleanFunc<T> booleanFunc) {
        int index = 0;
        for (T t : list) {
            if (booleanFunc.estimate(t)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    /**
     * 布尔函数接口,返回boolean值
     */
    @FunctionalInterface
    public interface BooleanFunc<T> {

        boolean estimate(T t);
    }

    /**
     * 累加函数接口,返回累加结果
     *
     * @param <T> 结果类型
     * @param <R> 元素类型
     */
    @FunctionalInterface
    public interface AccFunc<T, R> {

        T acc(T sum, R ele);
    }
}

// End Utilities.java
