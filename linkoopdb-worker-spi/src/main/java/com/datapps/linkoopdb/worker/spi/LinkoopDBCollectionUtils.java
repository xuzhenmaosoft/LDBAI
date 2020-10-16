/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import java.lang.reflect.Array;
import java.util.function.Consumer;
import java.util.function.Function;

import com.datapps.linkoopdb.jdbc.lib.Collection;
import com.datapps.linkoopdb.jdbc.lib.Iterator;

public class LinkoopDBCollectionUtils {

    public static <S, T> T[] collectBy(Collection list, Function<? super S, ? extends T> mapper, Class<T> tClass) {
        T[] a = (T[]) Array.newInstance(tClass, list.size());
        Iterator it = list.iterator();

        for (int i = 0; it.hasNext(); i++) {
            a[i] = mapper.apply((S) it.next());
        }

        return a;
    }

    public static void foreach(Collection list, Consumer c) {
        Iterator it = list.iterator();
        for (int i = 0; it.hasNext(); i++) {
            c.accept(it.next());
        }
    }

}
