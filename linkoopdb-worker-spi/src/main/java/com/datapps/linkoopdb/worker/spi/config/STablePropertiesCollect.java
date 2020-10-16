/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spi.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.datapps.linkoopdb.jdbc.TableBase;
import com.datapps.linkoopdb.jdbc.error.Error;
import com.datapps.linkoopdb.jdbc.error.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @auther webber
 * @date 2020/6/22 16:12
 */
public class STablePropertiesCollect<K, V> extends HashMap<K, V> {

    protected static final Logger logger = LoggerFactory.getLogger(STablePropertiesCollect.class);

    static final Map<String, PropertyDetail> PALLAS_PARAS = new HashMap<>();
    static final Map<String, PropertyDetail> DFS_PARAS = new HashMap<>();
    static final Map<String, PropertyDetail> EXTERNAL_TABLE_PARAS = new HashMap<>();
    static final Map<String, PropertyDetail> DBLINK_PARAS = new HashMap<>();
    static final Map<String, PropertyDetail> SYNC_PARAS = new HashMap<>();
    static final Map<String, PropertyDetail> STREAM_DFS_PARAS = new HashMap<>();
    static final Map<String, PropertyDetail> STREAM_PALLAS_PARAS = new HashMap<>();
    private static final Set<Map<String, PropertyDetail>> PARAS_SET;

    static {

        PARAS_SET = new HashSet<Map<String, PropertyDetail>>() {
            {
                add(PALLAS_PARAS);
                add(DFS_PARAS);
                add(EXTERNAL_TABLE_PARAS);
                add(DBLINK_PARAS);
                add(STREAM_DFS_PARAS);
                add(STREAM_PALLAS_PARAS);
                add(SYNC_PARAS);
            }
        };
    }

    public static void checkPropertiesCreateTable(TableBase sTable, Map<String, String> parameters) {
        checkCreateTable(sTable.getTableTypeString(), parameters, tableParametersFor(sTable));
    }

    public static void checkPropertiesAlterTable(TableBase sTable, String key, String value) {
        checkAlterTable(sTable.getTableTypeString(), key, value, tableParametersFor(sTable));
    }

    private static Map<String, PropertyDetail> tableParametersFor(TableBase sTable) {
        final Map<String, PropertyDetail> result;

        if (sTable.isExternal()) {
            result = EXTERNAL_TABLE_PARAS;
        } else if (sTable.isDBLink()) {
            result = DBLINK_PARAS;
        } else if (sTable.isDFS()) {
            if (sTable.isStream()) {
                result = STREAM_DFS_PARAS;
            } else {
                result = DFS_PARAS;
            }
        } else if (sTable.isPallas()) {
            if (sTable.isStream()) {
                result = STREAM_PALLAS_PARAS;
            } else if(sTable.isSync()) {
                result = SYNC_PARAS;
            } else {
                result = PALLAS_PARAS;
            }
        } else {
            result = new HashMap<>();
        }

        return result;
    }

    private static void checkCreateTable(String type, Map<String, String> parameters, Map<String, PropertyDetail> map) {
        if (map.isEmpty()) {
            return;
        }

        parameters.keySet().forEach(k -> {
            if (!map.containsKey(k)) {
                logger.error(type + " table can not define property: " + k);
                throw Error.error(ErrorCode.X_42555, type + " table can not define property: " + k, false);
            }
            verifyValue(parameters.get(k), map.get(k));
        });
    }

    private static void checkAlterTable(String type, String key, String value, Map<String, PropertyDetail> map) {
        if (map.isEmpty()) {
            return;
        }

        if (!map.containsKey(key)) {
            logger.error(type + " table can not define property: " + key);
            throw Error.error(ErrorCode.X_42555, type + " table can not define property: " + key, false);
        } else {
            PropertyDetail propertyDetail = map.get(key);
            if (!propertyDetail.isMutable()) {
                logger.error("Property " + key + " can not be modified.");
                throw Error.error(ErrorCode.X_42557, "Property " + key + " can not be modified.", true);
            }
            verifyValue(value, propertyDetail);
        }
    }

    private static void verifyValue(String value, PropertyDetail propertyDetail) {
        propertyDetail.checkValue(propertyDetail.valueOf(value));
    }

    @Override
    public V get(Object key) {
        V value = super.get(key);
        for (Map<String, PropertyDetail> paras : PARAS_SET) {
            if (value == null) {
                value = getDeprecatedConfig(paras.get(key));
            } else {
                break;
            }
        }
        return value;
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        V value = get(key);
        if (value != null) {
            return value;
        }
        return defaultValue;
    }

    @Override
    public boolean containsKey(Object key) {
        boolean isExists = super.containsKey(key);
        for (Map<String, PropertyDetail> paras : PARAS_SET) {
            if (!isExists) {
                isExists = containsProperty(paras, key);
            } else {
                break;
            }
        }
        return isExists;
    }

    private boolean containsProperty(Map<String, PropertyDetail> paras, Object key) {
        PropertyDetail detail = paras.get(key);
        if (detail != null) {
            for (Object deprecatedKey : detail.deprecatedKeys()) {
                if (super.containsKey(deprecatedKey)) {
                    return true;
                }
            }
        }
        return false;
    }

    private V getDeprecatedConfig(PropertyDetail option) {
        if (option != null) {
            for (Object o : option.deprecatedKeys()) {
                V value = super.get(o);
                if (value != null) {
                    return value;
                }
            }
        }
        return null;
    }
}
