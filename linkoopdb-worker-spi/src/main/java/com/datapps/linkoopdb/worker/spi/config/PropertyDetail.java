package com.datapps.linkoopdb.worker.spi.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import com.datapps.linkoopdb.jdbc.error.Error;
import com.datapps.linkoopdb.jdbc.error.ErrorCode;
import com.datapps.linkoopdb.monitor.common.util.Preconditions;
import com.datapps.linkoopdb.worker.spi.util.ClassTypeUtils;

/**
 * @author yukkit.zhang
 * @date 2020/8/20
 */
public class PropertyDetail<T> {

    private final String key;

    private final boolean mutable;

    private final String[] deprecatedKeys;

    private final T defaultValue;

    private final String errorMsg;

    private final DataTypeValidator<T> validator;

    private final String description;

    PropertyDetail(String key, boolean mutable, String description, T defaultValue, DataTypeValidator<T> validator, String errorMsg, String... deprecatedKeys) {
        this.key = Preconditions.checkNotNull(key);
        this.mutable = mutable;
        this.description = description;
        this.defaultValue = defaultValue;
        this.validator = validator;
        this.errorMsg = errorMsg;
        this.deprecatedKeys = deprecatedKeys;
    }

    /**
     * 数据规则检查
     * @param value
     *   T 类型的参数
     */
    public void checkValue(T value) {
        if (!validator.doValid(value)) {
            throw Error.error(ErrorCode.X_42556, errorMsg, false);
        }
    }

    /**
     * 数据类型检查
     * @param value
     *   任意字符串
     * @return
     *   被转换为与defaultValue类型一致的类型
     */
    public Object valueOf(String value) {
        try {
            return ClassTypeUtils.cast(defaultValue.getClass(), value);
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_42561, value + " to " + defaultValue.getClass().getSimpleName(), false);
        }
    }

    public String key() {
        return key;
    }

    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    public T defaultValue() {
        return defaultValue;
    }

    public boolean hasDeprecatedKeys() {
        return deprecatedKeys.length != 0;
    }

    public Iterable<String> deprecatedKeys() {
        return deprecatedKeys.length == 0 ? Collections.<String>emptyList() : Arrays.asList(deprecatedKeys);
    }

    public String description() {
        return description;
    }

    public boolean isMutable() {
        return mutable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PropertyDetail<?> that = (PropertyDetail<?>) o;
        return Objects.equals(key, that.key)
            && Arrays.equals(deprecatedKeys, that.deprecatedKeys)
            && Objects.equals(defaultValue, that.defaultValue)
            && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(key, defaultValue, description);
        result = 31 * result + Arrays.hashCode(deprecatedKeys);
        return result;
    }
}
