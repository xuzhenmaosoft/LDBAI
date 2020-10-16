package com.datapps.linkoopdb.worker.spi.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 定义所有的表参数
 * @author yukkit.zhang
 * @date 2020/8/20
 */
public class TableParameters {

    @SafeVarargs
    public static <T> Set<T> set(T... paras) {
        return new HashSet<>(Arrays.asList(paras));
    }

    static final byte DFS = 1;
    static final byte PALLAS = 2;
    static final byte EXT = 4;
    static final byte DBLINK = 8;
    static final byte STREAM = 16;
    static final byte SYNC = 32;

    // all table
    public static final PropertyDetail<Boolean> binlog = ConfigOptions
        .key("binlog")
        .defaultValue(false)
        .withDescription("whether to write binlog")
        .build(DFS | PALLAS);

    public static final PropertyDetail<String> studioSensitive = ConfigOptions
        .key("sensitive")
        .defaultValue("0")
        .withValidator(v -> set("0", "1").contains(v), "case sensitive or not create by studio, default is 0")
        .withDescription("case sensitive or not create by studio, default is 0")
        .build(DFS | PALLAS);

    public static final PropertyDetail<Integer> insertCacheCount = ConfigOptions
        .key("insert.cache.count")
        .defaultValue(-1)
        .withValidator(v -> v >= 1 && v <= 10000, " value must be greater than or equal to 1 and less than or equal to 10000")
        .withDescription("number of cache for a flush operation by insert-values")
        .build(DFS | PALLAS);

    // pallas
    public static final PropertyDetail<Boolean> newKeyRule = ConfigOptions
        .key("newkeyrule")
        .defaultValue(false)
        .withDescription("whether use 22 bytes key rule for pallas table, default is true")
        .build(PALLAS);

    public static final PropertyDetail<Boolean> upsert = ConfigOptions
        .key("upsert")
        .defaultValue(false)
        .withDescription("whether pallas table use upsert mode to insert")
        .build(PALLAS);

    public static final PropertyDetail<Integer> txnNum = ConfigOptions
        .key("txnnum")
        .defaultValue(-1)
        .withValidator(v -> v >= 1 && v <= 1000000, " value must be greater than or equal to 1 and less than or equal to 1000000")
        .withDescription("rows number of a insert grpc call in txn")
        .build(PALLAS);

    public static final PropertyDetail<Integer> perIngestNum = ConfigOptions
        .key("peringestnum")
        .defaultValue(-1)
        .withValidator(v -> v >= 1 && v <= 1000000, " value must be greater than or equal to 1 and less than or equal to 1000000")
        .withDescription("rows number of a ingest grpc call")
        .build(PALLAS);

    public static final PropertyDetail<Integer> perInsertNum = ConfigOptions
        .key("perinsertnum")
        .defaultValue(-1)
        .withValidator(v -> v >= 1 && v <= 100000, " value must be greater than or equal to 1 and less than or equal to 100000")
        .withDescription("rows number of a insert grpc call")
        .build(PALLAS);

    public static final PropertyDetail<Integer> shardNum = ConfigOptions
        .key("shards")
        .defaultValue(-1)
        .withMutable(false)
        .withValidator(v -> v >= 1 && v <= 4096, " value must be greater than or equal to 1 and less than or equal to 4096")
        .withDeprecatedKeys(
            "ldb.storage.pallas.shardNumber",
            "linkoopdb.pallas.shard_number")
        .withDescription("pallas shard number of one table")
        .build(PALLAS);

    public static final PropertyDetail<Integer> shardDupNum = ConfigOptions
        .key("replicas")
        .defaultValue(-1)
        .withValidator(v -> v >= 1 && v <= 10, " value must be greater than or equal to 1 and less than or equal to 10")
        .withDeprecatedKeys(
            "ldb.storage.pallas.shardDuplicateNumber",
            "linkoopdb.pallas.shard_duplicate_number")
        .withDescription("duplicate number of a pallas table shard")
        .build(PALLAS);

    public static final PropertyDetail<Integer> minShardDupNum = ConfigOptions
        .key("min_replicas")
        .defaultValue(-1)
        .withValidator(v -> v >= 1 && v <= 10, " value must be greater than or equal to 1 and less than or equal to 10")
        .withDeprecatedKeys(
            "ldb.storage.pallas.minShardDuplicateNumber",
            "linkoopdb.pallas.min_shard_duplicate_number")
        .withDescription("minimum duplicate number of a pallas table shard")
        .build(PALLAS);

    public static final PropertyDetail<Boolean> dictCompress = ConfigOptions
        .key("dictcompress")
        .defaultValue(false)
        .withDeprecatedKeys(
            "ldb.storage.pallas.dictCompressEnabled",
            "linkoopdb.pallas.dict_compress_enabled")
        .withDescription("whether use dictionary compression of a pallas table, default is false")
        .build(PALLAS);

    public static final PropertyDetail<String> storageOri = ConfigOptions
        .key("orientation")
        .defaultValue("row")
        .withMutable(false)
        .withValidator(e -> set("row", "column").contains(e), " must be row or column")
        .withDeprecatedKeys(
            "ldb.storage.pallas.storageOrientation",
            "linkoopdb.pallas.storage.orientation")
        .withDescription("pallas table uses row store or column store, default is row store")
        .build(PALLAS);

    public static final PropertyDetail<Boolean> columnApp = ConfigOptions
        .key("appendonly")
        .defaultValue(false)
        .withMutable(false)
        .withDeprecatedKeys(
            "ldb.storage.pallas.storageColumnAppendonly",
            "linkoopdb.pallas.storage.column.appendonly")
        .withDescription("whether pallas table is appendonly table")
        .build(PALLAS);

    // hdfs
    public static final PropertyDetail<Integer> compactDegree = ConfigOptions
        .key("compact.degree")
        .defaultValue(64)
        .withValidator(v -> v >= 1, " value must be greater than or equal 1 ")
        .withDescription("compact degree of a hdfs table")
        .build(DFS);

    public static final PropertyDetail<Integer> compactParallel = ConfigOptions
        .key("compact.parallel")
        .defaultValue(-1)
        .withValidator(v -> v >= 1, " value must be greater than or equal 1 ")
        .withDescription("compact parallel of a hdfs table")
        .build(DFS);

    public static final PropertyDetail<Integer> writeParallelism = ConfigOptions
        .key("write_parallelism")
        .defaultValue(200)
        .withValidator(v -> v >= 1, " value must be greater than or equal 1 ")
        .withDescription("write parallelism of a table in spark")
        .build(DFS);

    public static final PropertyDetail<String> parquetCompression = ConfigOptions
        .key("compression")
        .defaultValue("none")
        .withMutable(false)
        .withValidator(e -> set("none", "snappy", "gzip", "lz4").contains(e), " value must be one of [none, snappy, gzip, lz4]")
        .withDescription("compression codec of hdfs parquet table")
        .build(DFS);

    public static final PropertyDetail<String> indexBloomFilterFPP = ConfigOptions
        .key("bloom.fpp")
        .noDefaultValue()
        .withDescription("index bloom filter fpp")
        .build(DFS);

    public static final PropertyDetail<String> metastoreLocation = ConfigOptions
        .key("index.metastore")
        .noDefaultValue()
        .withDescription("index file location")
        .build(DFS);

    public static final PropertyDetail<Integer> indexNumPartitions = ConfigOptions
        .key("index.partitions")
        .defaultValue(-1)
        .withDescription("index Parallelism num partitions")
        .build(DFS);

    public static final PropertyDetail<Boolean> indexStatisticsEnable = ConfigOptions
        .key("index.parquet.filter.enabled")
        .defaultValue(false)
        .withDescription("writes filter statistics for indexed columns,otherwise only min/max statistics are used,default is true")
        .build(DFS);

    public static final PropertyDetail<String> indexStatisticsType = ConfigOptions
        .key("index.parquet.filter.type")
        .noDefaultValue()
        .withValidator(e -> set("bloom", "dict").contains(e), "")
        .withDescription("type of statistics to use when creating index, {bloom, dict}, default is bloom")
        .build(DFS);

    public static final PropertyDetail<Boolean> indexStatisticsEagerLoad = ConfigOptions
        .key("index.parquet.filter.eagerLoading")
        .defaultValue(false)
        .withDescription("When set to true, read and load all filter statistics in memory the first time catalog is resolved, default is false")
        .build(DFS);

    public static final PropertyDetail<Integer> maxBloomFileSize = ConfigOptions
            .key("index.bloom.size")
            .defaultValue(500)
            .withDescription("max bloom file, When this configuration is used, the FPP value is automatically "
                    + "configured and the bloom.fpp value is invalidated if it is set")
            .build(DFS);

    public static final PropertyDetail<String> syncLinkDbTargetTable = ConfigOptions
        .key("sync.link.db.target.table")
        .noDefaultValue()
        .withDescription("When set to true, read and load all filter statistics in memory the first time catalog is resolved, default is false")
        .build(SYNC | PALLAS);
}
