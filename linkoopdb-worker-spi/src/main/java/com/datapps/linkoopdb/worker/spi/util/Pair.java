package com.datapps.linkoopdb.worker.spi.util;

import java.io.Serializable;
import java.util.Objects;


/**
 * @author xingbu
 * @version 1.0
 *
 * created by　20-1-22 下午2:46
 */
public class Pair<K, V> implements Serializable {

    private K key;

    public K getKey() {
        return key;
    }

    private V value;

    public V getValue() {
        return value;
    }

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }

    @Override
    public int hashCode() {
        return key.hashCode() * 13 + (value == null ? 0 : value.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof Pair) {
            Pair pair = (Pair) o;
            if (!Objects.equals(key, pair.key)) {
                return false;
            }
            if (!Objects.equals(value, pair.value)) {
                return false;
            }
            return true;
        }
        return false;
    }

}
