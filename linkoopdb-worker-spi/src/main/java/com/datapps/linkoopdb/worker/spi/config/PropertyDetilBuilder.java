package com.datapps.linkoopdb.worker.spi.config;

import static com.datapps.linkoopdb.worker.spi.config.TableParameters.DBLINK;
import static com.datapps.linkoopdb.worker.spi.config.TableParameters.DFS;
import static com.datapps.linkoopdb.worker.spi.config.TableParameters.EXT;
import static com.datapps.linkoopdb.worker.spi.config.TableParameters.PALLAS;
import static com.datapps.linkoopdb.worker.spi.config.TableParameters.STREAM;
import static com.datapps.linkoopdb.worker.spi.config.TableParameters.SYNC;

import com.datapps.linkoopdb.monitor.common.util.Preconditions;

/**
 * @author yukkit.zhang
 * @date 2020/8/21
 */
public class PropertyDetilBuilder<T> {
    private String key;
    private boolean mutable;
    private String[] deprecatedKeys;
    private T defaultValue;
    private String errorMsg;
    private DataTypeValidator<T> validator;
    private String description;

    PropertyDetilBuilder(String key, T defaultValue) {
        this.key = Preconditions.checkNotNull(key);
        this.mutable = true;
        this.description = "";
        this.defaultValue = defaultValue;
        this.validator = e -> true;
        this.errorMsg = "";
        this.deprecatedKeys = new String[0];
    }

    public PropertyDetilBuilder<T> withDeprecatedKeys(final String... deprecatedKeys) {
        this.deprecatedKeys = deprecatedKeys;
        return this;
    }

    public PropertyDetilBuilder<T> withDescription(final String description) {
        this.description = description;
        return this;
    }

    public PropertyDetilBuilder<T> withValidator(final DataTypeValidator<T> validator, String errorMsg) {
        this.validator = validator;
        this.errorMsg = errorMsg;
        return this;
    }

    public PropertyDetilBuilder<T> withMutable(final boolean mutable) {
        this.mutable = mutable;
        return this;
    }

    public PropertyDetail<T> build(int type) {
        PropertyDetail<T> propertyDetail = new PropertyDetail<>(key, mutable, description, defaultValue, validator, errorMsg, deprecatedKeys);
        if ((type & DFS) != 0) {
            STablePropertiesCollect.DFS_PARAS.put(propertyDetail.key(), propertyDetail);
            if ((type & STREAM) != 0) {
                STablePropertiesCollect.STREAM_DFS_PARAS.put(propertyDetail.key(), propertyDetail);
            }
        }
        if ((type & PALLAS) != 0) {
            STablePropertiesCollect.PALLAS_PARAS.put(propertyDetail.key(), propertyDetail);
            if ((type & STREAM) != 0) {
                STablePropertiesCollect.STREAM_PALLAS_PARAS.put(propertyDetail.key(), propertyDetail);
            }
        }
        if ((type & EXT) != 0) {
            STablePropertiesCollect.EXTERNAL_TABLE_PARAS.put(propertyDetail.key(), propertyDetail);
        }
        if ((type & DBLINK) != 0) {
            STablePropertiesCollect.DBLINK_PARAS.put(propertyDetail.key(), propertyDetail);
        }

        if ((type & SYNC) != 0) {
            STablePropertiesCollect.SYNC_PARAS.put(propertyDetail.key(), propertyDetail);
        }

        return propertyDetail;
    }
}
