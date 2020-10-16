package com.datapps.linkoopdb.worker.spi.plan.expression;

/**
 * Created by gloway on 2019/1/29.
 */
public enum SqlKind {

    COLUMN,
    COALESCE,
    CIRCLE_CONTAIN,
    DISTANCE_WITHIN,
    DEFAULT,
    SIMPLE_COLUMN,
    VARIABLE,
    PARAMETER,
    DYNAMIC_PARAM,
    TRANSITION_VARIABLE,
    DIAGNOSTICS_VARIABLE,
    ASTERISK,
    SEQUENCE,
    SEQUENCE_CURRENT,
    ROWNUM,
    ARRAY,
    MULTISET,
    SCALAR_SUBQUERY,
    ROW_SUBQUERY,
    TABLE_SUBQUERY,
    TABLE_PARAMETER,
    RECURSIVE_SUBQUERY,
    ROW,
    VALUELIST,
    FUNCTION,
    SQL_FUNCTION,
    STATE_FUNCTION,
    TABLE,
    NEGATE,
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    LIKE_ARG,
    CASEWHEN_COALESCE,
    IS_NOT_NULL,
    EQUAL("="),
    GREATER_EQUAL(">="),
    GREATER_EQUAL_PRE,
    GREATER(">"),
    SMALLER("<"),
    SMALLER_EQUAL("<="),

    SMALL_EQUAL_GREATER,

    NOT_EQUAL("<>"),
    IS_NULL,
    NOT,
    AND,
    OR,
    ALL_QUANTIFIED,
    ANY_QUANTIFIED,
    LIKE,
    IN,
    EXISTS,
    RANGE_CONTAINS,
    RANGE_EQUALS,
    RANGE_OVERLAPS,
    RANGE_PRECEDES,
    RANGE_SUCCEEDS,
    RANGE_IMMEDIATELY_PRECEDES,
    RANGE_IMMEDIATELY_SUCCEEDS,
    UNIQUE,
    NOT_DISTINCT,
    MATCH_SIMPLE,
    MATCH_PARTIAL,
    MATCH_FULL,
    MATCH_UNIQUE_SIMPLE,
    MATCH_UNIQUE_PARTIAL,
    MATCH_UNIQUE_FULL,
    COUNT,
    SUM,

    MYAGG,

    MIN,
    MAX,
    AVG,
    EVERY,
    SOME,
    STDDEV_POP,
    STDDEV_SAMP,
    STD,
    STDDEV,
    VAR_POP,
    VAR_SAMP,
    VARIANCE,
    ARRAY_AGG,
    GROUP_CONCAT,
    GROUP_TO_ARRAY,
    PREFIX,
    MEDIAN,
    CONCAT_WS,
    CAST,
    ZONE_MODIFIER,
    CASEWHEN,
    ORDER_BY,
    LIMIT,
    ALTERNATIVE,
    MULTICOLUMN,
    USER_AGGREGATE,
    ARRAY_ACCESS,
    ARRAY_SUBQUERY,

    WINDOW,
    WINDOW_FUNCTION,

    //    aggregate functions
    SKEW,
    PIVOT,
    UNPIVOT,
    STREAM_CONCAT_WS,

    // spark udf aggr function
    APPROX_COUNT_DISTINCT,
    APPROX_PERCENTILE,
    CORR,
    COVAR_POP,
    COVAR_SAMP,
    KURTOSIS,
    PERCENTILE_APPROX,
    REGR_SLOPE,
    REGR_INTERCEPT,
    REGR_COUNT,
    REGR_R2,
    REGR_SXX,
    REGR_SYY,
    REGR_AVGX,
    REGR_AVGY,
    REGR_SXY,

    // spark other function
    JSON_PATH,
    TEST_EXECUTE,
    INPUT_FILE_NAME,
    DATE_TRUNC,
    BINARY,
    BINARY_TO_DOUBLE_ARRAY,
    FORMAT_STRING,
    ELT,
    NAMED_STRUCT,
    JAVA_DECODE,
    ISNULL,
    REGEXP_EXTRACT,
    ARRAY_CONTAINS,
    ARRAY_POSITION,
    XPATH,
    XPATH_BOOLEAN,
    XPATH_DOUBLE,
    XPATH_FLOAT,
    XPATH_INT,
    XPATH_LONG,
    XPATH_NUMBER,
    XPATH_SHORT,
    XPATH_STRING,
    ASSERT_TRUE,
    AVG_WHERE,
    AVG_CATE_WHERE,
    BASE64,
    BIGINT,
    BIN,
    BOOLEAN,
    BROUND,
    CBRT,
    CHECKOVERFLOW,
    CNT_CATE_WHERE,
    COUNT_WHERE,
    CNT_OF_CATE,
    COLLECT_LIST,
    COLLECT_SET,
    CONTAINS,
    CONV,
    COUNT_MIN_SKETCH,
    CRC32,
    CUBE,
    CURRENT_DATABASE,
    CVTCOLOR,
    DATE_FORMAT,
    DAY,
    DECIMAL,
    DOUBLE,
    E,
    ENCODE,
    ENCODE_IMAGE,
    EXPLODE,
    EXPLODE_OUTER,
    EXPM1,
    FACTORIAL,
    FILE_TO_BINARY,
    FIND_IN_SET,
    FLOAT,
    FORMAT_NUMBER,
    FROM_JSON,
    FROM_UNIXTIME,
    FROM_UTC_TIMESTAMP,
    GET_JSON_OBJECT,
    GROUPING_ID,
    HASH,
    HEX,
    HOUR,
    HYPOT,
    IF,
    INITCAP,
    INLINE,
    INLINE_OUTER,
    INPUT_FILE_BLOCK_LENGTH,
    INPUT_FILE_BLOCK_START,
    INT,
    ISNAN,
    ISNOTNULL,
    JAVA_METHOD,
    JSON_TUPLE,
    LEVENSHTEIN,
    LOG1P,
    LOG2,
    MAP,
    POLYGON_CONTAIN,
    MAP_KEYS,
    MAP_VALUES,
    MD5,
    MEAN,
    MONOTONICALLY_INCREASING_ID,
    NEGATIVE,
    PARSE_URL,
    PERCENTILE,
    PMOD,
    POSEXPLODE,
    POSEXPLODE_OUTER,
    POSITIVE,
    PRINTF,
    PROMOTEPRECISION,
    QUARTER,
    RANDN,
    RECT_CONTAIN,
    REFLECT,
    REPLICATEROWS,
    RESIZE,
    RINT,
    RLIKE,
    SENTENCES,
    SHA,
    SHA1,
    SHA2,
    SHIFTLEFT,
    SHIFTRIGHT,
    SHIFTRIGHTUNSIGNED,
    SIGNUM,
    SIZE,
    SKEWNESS,
    SMALLINT,
    SPARK_PARTITION_ID,
    SPLIT,
    STACK,
    STR_TO_MAP,
    STRING,
    STRUCT,
    SUBSTRING_INDEX,
    SUM_WHERE,
    SUM_CATE_WHERE,
    TOPN_SUM_CATE_WHERE,
    TOPN_CNT_CATE_WHERE,
    TOPN_CATE_ORDERBY_SUM_WHERE,
    TOPN_CATE_ORDERBY_CNT_WHERE,
    LOG_CNT_CATE_WHERE,
    LOG_SUM_CATE_WHERE,
    MAX_N_WHERE,
    MIN_N_WHERE,
    TINYINT,
    TO_JSON,
    TO_UNIX_TIMESTAMP,
    TO_UTC_TIMESTAMP,
    UNBASE64,
    UNHEX,
    WEEKOFYEAR,
    YEAR,
    VECTOR,
    TUMBLE_START,
    TUMBLE_END,
    TUMBLE_ROWTIME,
    TUMBLE_PROCTIME,
    TUMBLE,
    HOP_START,
    HOP_END,
    HOP_ROWTIME,
    HOP_PROCTIME,
    HOP,
    SESSION_START,
    SESSION_END,
    SESSION_ROWTIME,
    SESSION_PROCTIME,
    SESSION,
    SYS_STREAM_SNAPSHOT,
    SYS_STREAM_RETRIEVE_RESULT,

    // udf
    UDF,
    UDAF,

    PYTHONUDF,

    // Function Sql
    POSITION_CHAR,
    POSITION_BINARY,
    OCCURENCES_REGEX,
    POSITION_REGEX,
    EXTRACT,
    BIT_LENGTH,
    CHAR_LENGTH,
    OCTET_LENGTH,
    CARDINALITY,
    MAX_CARDINALITY,
    TRIM_ARRAY,
    ABS,
    MOD,
    LN,
    EXP,
    POWER,
    SQRT,
    FLOOR,
    CEILING,
    WIDTH_BUCKET,
    SUBSTRING_CHAR,
    SUBSTRING_REG_EXPR,
    SUBSTRING_REGEX,
    FOLD_LOWER,
    FOLD_UPPER,
    TRANSCODING,
    TRANSLITERATION,
    REGEX_TRANSLITERATION,
    TRIM_CHAR,
    OVERLAY_CHAR,
    CHAR_NORMALIZE,
    SUBSTRING_BINARY,
    TRIM_BINARY,
    OVERLAY_BINARY,
    CURRENT_DATE,
    CURRENT_TIME,
    CURRENT_TIMESTAMP,
    LOCALTIME,
    LOCALTIMESTAMP,
    VALUE,
    GROUPING,
    NEW,

    // fuction custom
    ACOS,
    ACTION_ID,
    ADD_MONTHS,
    ASCII,
    ASIN,
    ATAN,
    ATAN2,
    BITAND,
    BITANDNOT,
    BITNOT,
    BITOR,
    BITXOR,
    CHAR,
    CONCAT,
    COS,
    COSH,
    COT,
    CRYPT_KEY,
    DATABASE,
    DATABASE_ISOLATION_LEVEL,
    DATABASE_NAME,
    DATABASE_TIMEZONE,
    DATABASE_VERSION,
    DATE_ADD,
    DATE_SUB,
    DATEADD,
    DATEDIFF,
    DAYS,
    DBTIMEZONE,
    DEGREES,
    DIAGNOSTICS,
    DIFFERENCE,
    FROM_TZ,
    HEXTORAW,
    IDENTITY,
    INSTR,
    ISAUTOCOMMIT,
    ISOLATION_LEVEL,
    ISREADONLYDATABASE,
    ISREADONLYDATABASEFILES,
    ISREADONLYSESSION,
    LAST_DAY,
    LEFT,
    LOAD_FILE,
    LOB_ID,
    LOCATE,
    LOG10,
    LPAD,
    LTRIM,
    MONTHS_BETWEEN,
    NEW_TIME,
    NEXT_DAY,
    NUMTODSINTERVAL,
    NUMTOYMINTERVAL,
    PI,
    POSITION_ARRAY,
    RADIANS,
    RAND,
    RAWTOHEX,
    REGEXP_MATCHES,
    REGEXP_REPLACE,
    REGEXP_SUBSTRING,
    REGEXP_SUBSTRING_ARRAY,
    MYTEST,
    POW,
    FUNCTIONDESC,
    REBOOT,
    RESTARTSERVER,
    REPEAT,
    REPLACE,
    REVERSE,
    RIGHT,
    ROUND,
    ROUNDMAGIC,
    RPAD,
    RTRIM,
    SECONDS_MIDNIGHT,
    SEQUENCE_ARRAY,
    EXT_ARRAY,
    SESSION_ID,
    SESSION_ISOLATION_LEVEL,
    SESSION_TIMEZONE,
    SESSIONTIMEZONE,
    SIGN,
    SIN,
    SINH,
    SOUNDEX,
    SORT_ARRAY,
    SPACE,
    SUBSTR,
    SYS_EXTRACT_UTC,
    SYSDATE,
    SYSTIMESTAMP,
    TAN,
    TANH,
    TIMESTAMP,
    TIMESTAMP_WITH_ZONE,
    TIMESTAMPADD,
    TIMESTAMPDIFF,
    TIMEZONE,
    TO_CHAR,
    TO_DATE,
    TO_DSINTERVAL,
    TO_YMINTERVAL,
    TO_NUMBER,
    TO_TIMESTAMP,
    TO_TIMESTAMP_TZ,
    TRANSACTION_CONTROL,
    TRANSACTION_ID,
    TRANSACTION_SIZE,
    TRANSLATE,
    TRUNC,
    TRUNCATE,
    UUID,
    UNIX_TIMESTAMP,
    UNIX_MILLIS,
    DATEPART,
    DATENAME,
    NANVL,
    SQLCODE,
    SQLERRM,
    STREAM_FUNC_DATE_ADD,
    STREAM_FUNC_DATE_SUB,
    STREAM_FUNC_CHAR,
    RANK,
    DENSE_RANK,
    PERCENT_RANK,
    CUME_DIST,
    ROW_NUMBER,
    NTILE,
    LAG,
    LEAD,
    FIRST,
    FIRST_VALUE,
    LAST,
    LAST_VALUE,
    NTH_VALUE,
    PREV,
    NEXT,
    JSON_TABLE,
    EXTENDED_WINDOW_FUNCTION,

    //ai fitting functions
    DECISION_TREE_TRAIN,
    GBTCLASSIFIER_TRAIN,
    LINEARSVC_TRAIN,
    LOGISTICREGRESSION_TRAIN,
    MULTILAYER_PERCEPTRON_CLASSIFIER_TRAIN,
    NAIVE_BAYES_TRAIN,
    ONEVSREST_TRAIN,
    RANDOM_FOREST_CLASSIFIER_TRAIN,
    BISECTING_KMEANS_TRAIN,
    GAUSSIAN_MIXTURE_TRAIN,
    KMEANS_TRAIN,
    LDA_TRAIN,
    HASHINGTF_TRANSFORMER,
    IDF_TRANSFORMER,
    TOKENIZER_TRANSFORMER,
    CONCAT_STR_WHERE,
    CONCAT_STR_LAST_K_WHERE,
    COUNTVECTORIZER_TRANSFORMER,
    FEATUREHASHER_TRANSFORMER,
    WORD2VEC_TRANSFORMER,
    CHISQSELECTOR_TRANSFORMER,
    RFORMULA_TRANSFORMER,
    VECTORSLICER_TRANSFORMER,
    BINARIZER_TRANSFORMER,
    BUCKETIZER_TRANSFORMER,
    DCT_TRANSFORMER,
    ELEMENT_WISE_PRODUCT_TRANSFORMER,
    IMPUTER_TRANSFORMER,
    INDEX_TO_STRING_TRANSFORMER,
    MAXABS_SCALER_TRANSFORMER,
    MINMAX_SCALER_TRANSFORMER,
    NGRAM_TRANSFORMER,
    NORMALIZER_TRANSFORMER,
    ONEHOTENCODER_ESTIMATOR_TRANSFORMER,
    PCA_TRANSFORMER,
    POLYNOMIAL_EXPANSION_TRANSFORMER,
    QUANTILE_DISCRETIZER_TRANSFORMER,
    STANDARD_SCALER_TRANSFORMER,
    STOP_WORDS_REMOVER_TRANSFORMER,
    STRING_INDEXER_TRANSFORMER,
    VECTOR_ASSEMBLER_TRANSFORMER,
    VECTOR_INDEXER_TRANSFORMER,
    VECTOR_SIZE_HINT_TRANSFORMER,
    DECISION_TREE_REGRESSION_TRAIN,
    GBTREGRESSION_TRAIN,
    ISOTONIC_REGRESSION_TRAIN,
    LINEAR_REGRESSION_TRAIN,
    RANDOM_FOREST_REGRESSION_TRAIN,
    SURVIVAL_REGRESSION_TRAIN,
    ALS_TRAIN,
    LOAD_EXTERNAL_MODEL,
    TENSORFLOW_MODEL_TRAIN,
    FPGROWTH_TRAIN,
    GLR_TRAIN,
    TRAIN_VALIDATION_SPLIT,

    //ldb ai  transform functions
    DECISION_TREE_PREDICT,
    GBTCLASSIFIER_PREDICT,
    LINEARSVC_PREDICT,
    LOGISTICREGRESSION_PREDICT,
    MULTILAYER_PERCEPTRON_CLASSIFIER_PREDICT,
    NAIVE_BAYES_PREDICT,
    ONEVSREST_PREDICT,
    RANDOM_FOREST_CLASSIFIER_PREDICT,
    BISECTING_KMEANS_PREDICT,
    GAUSSIAN_MIXTURE_PREDICT,
    KMEANS_PREDICT,
    LDA_PREDICT,
    HASHINGTF,
    IDF,
    TOKENIZER,
    COUNTVECTORIZER,
    FEATUREHASHER,
    WORD2VEC,
    CHISQSELECTOR,
    RFORMULA,
    VECTORSLICER,
    BINARIZER,
    BUCKETIZER,
    DCT,
    ELEMENT_WISE_PRODUCT,
    IMPUTER,
    INDEX_TO_STRING,
    MAXABS_SCALER,
    MINMAX_SCALER,
    NGRAM,
    NORMALIZER,
    ONEHOTENCODER_ESTIMATOR,
    PCA,
    POLYNOMIAL_EXPANSION,
    QUANTILE_DISCRETIZER,
    STANDARD_SCALER,
    STOP_WORDS_REMOVER,
    STRING_INDEXER,
    VECTOR_ASSEMBLER,
    VECTOR_INDEXER,
    VECTOR_SIZE_HINT,
    DECISION_TREE_REGRESSION_PREDICT,
    GBTREGRESSION_PREDICT,
    ISOTONIC_REGRESSION_PREDICT,
    LINEAR_REGRESSION_PREDICT,
    RANDOM_FOREST_REGRESSION_PREDICT,
    SURVIVAL_REGRESSION_PREDICT,
    ALS_RECOMMEND,
    ALS_PREDICT,
    ALS_ITEM_USER_FACTORS,
    PREDICT_BY_EXTERNAL_MODEL,
    GLR_PREDICT,
    TENSORFLOW_MODEL_PREDICT,
    TRAIN_VALIDATION_PREDICT,

    //ai evaluate functions
    PRECISIONBYTHRESHOLD,
    RECALLBYTHRESHOLD,
    FMEASUREBYTHRESHOLD,
    PR,
    ROC,
    AUPRC,
    AUROC,
    SILHOUETTESCORE,
    FPGROWTH_FREQ,
    FPGROWTH_CONFIDENCE,
    GLR_STATISTIC,
    KMEANS_WSSSE,
    BISECTING_KMEANS_WSSSE,
    ACCURACY,
    WEIGHTEDFMEASURE,
    WEIGHTEDRECALL,
    WEIGHTEDPRECISION,
    FPRBYLABEL,
    WEIGHTEDFPR,
    FMEASUREBYLABEL,
    PRECISIONBYLABEL,
    RECALLBYLABEL,
    CONFUSIONMATRIX,
    MLPRECISION,
    MLPRECISIONBYLABEL,
    MLRECALL,
    MLRECALLBYLABEL,
    MLACCURACY,
    MLFMEASURE,
    HAMMINGLOSS,
    MICROFMEASURE,
    MICROPRECISION,
    MICRORECALL,
    SUBSETACCURACY,
    MLFMEASUREBYLABEL,
    PRECISIONAT,
    MEANAVERAGEPRECISION,
    NDCGAT,
    MSE,
    RMSE,
    R2,
    MAE,

    //ai graph functions
    GRAPH_BUILD,
    GRAPH_TRIANGLE_COUNT,
    GRAPH_STRONGLY_CONNECTED_COMPONENTS,
    GRAPH_LABEL_PROPAGATION,
    GRAPH_N_DEGREE,
    GRAPH_N_DEGREE_ALL,
    GRAPH_BFS,
    GRAPH_SHORTEST_PATHS,
    GRAPH_PAGE_RANK,
    GRAPH_PAGE_RANK_PREGEL,
    GRAPH_CONNECTED_COMPONENTS,

    //pipeline functions
    PIPELINE_BUILD,
    PIPELINE_APPLY,

    // spark op
    REPARTITION,
    REPARTITION_BY_COUNT,

    RETRIEVEBYSOURCESQL,

    CURRENT_ROW,
    UNBOUNDED_PRECEDING,
    PRECEDING,
    UNBOUNDED_FOLLOWING,
    FOLLOWING,

    TYPE,

    STRUCT_ACCESS,

    JSON_TO_STRUCT,
    STRUCT_TO_JSON,

    GEO_DECODE,
    GEO_ENCODE,

    ARRAY_TO_STRING,

    FINAL,
    RUNNING,
    PATTERN_ALTER,
    PATTERN_CONCAT,
    PATTERN_EXCLUDE,
    PATTERN_QUANTIFIER,
    PATTERN_PERMUTE,
    SKIP_TO_FIRST,
    SKIP_TO_LAST,

    GROUP_BY;

    public String sql;

    SqlKind() {
        sql = name();
    }

    SqlKind(String sql) {
        this.sql = sql;
    }

    public static boolean isPipelineApply(SqlKind sqlKind) {
        return sqlKind.equals(PIPELINE_APPLY);
    }

    public static boolean isLdbFunction(SqlKind sqlKind) {
        return isAiFunction(sqlKind) || isGraphFunction(sqlKind);
    }

    public static boolean isLdbFunction(String sql) {
        try {
            convertLdbFunctionByName(sql);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean isSparkOp(SqlKind sqlKind) {
        return sqlKind.equals(REPARTITION) || sqlKind.equals(REPARTITION_BY_COUNT);
    }

    public static boolean isAiFunction(SqlKind sqlKind) {
        return sqlKind.compareTo(DECISION_TREE_TRAIN) >= 0 && sqlKind.compareTo(MAE) <= 0;
    }


    public static boolean isAiTrain(SqlKind sqlKind) {
        return sqlKind.compareTo(DECISION_TREE_TRAIN) >= 0
            && sqlKind.compareTo(TRAIN_VALIDATION_SPLIT) <= 0;
    }

    public static boolean isAiTransform(SqlKind sqlKind) {
        return sqlKind.compareTo(DECISION_TREE_PREDICT) >= 0
            && sqlKind.compareTo(TRAIN_VALIDATION_PREDICT) <= 0;
    }

    public static boolean isAiEvaluate(SqlKind sqlKind) {
        return sqlKind.compareTo(PRECISIONBYTHRESHOLD) >= 0 && sqlKind.compareTo(MAE) <= 0;
    }

    public static boolean isGraphFunction(SqlKind sqlKind) {
        return sqlKind.compareTo(GRAPH_BUILD) >= 0
            && sqlKind.compareTo(GRAPH_CONNECTED_COMPONENTS) <= 0;
    }

    public static boolean isBinaryArithmetic(int opType) {
        switch (opType) {
            case 32:
            case 78:
            case 81:
            case 82:
            case 35:
            case 34:
            case 13:
            case 33:
                return true;
            default:
                return false;
        }
    }

    public static boolean isGraphTrain(SqlKind sqlKind) {
        return sqlKind.equals(GRAPH_BUILD) || sqlKind.equals(GRAPH_PAGE_RANK) || sqlKind.equals(GRAPH_PAGE_RANK_PREGEL);
    }

    public static boolean isGraphCompute(SqlKind sqlKind) {
        return sqlKind.equals(GRAPH_TRIANGLE_COUNT) || sqlKind.equals(GRAPH_CONNECTED_COMPONENTS)
            || sqlKind.equals(GRAPH_STRONGLY_CONNECTED_COMPONENTS) || sqlKind.equals(GRAPH_LABEL_PROPAGATION) || sqlKind.equals(GRAPH_N_DEGREE)
            || sqlKind.equals(GRAPH_N_DEGREE_ALL) || sqlKind.equals(GRAPH_BFS) || sqlKind.equals(GRAPH_SHORTEST_PATHS);
    }

    public static SqlKind convertLdbFunctionByName(String name) {
        switch (name) {
            case "DECISION_TREE_TRAIN":
                return DECISION_TREE_TRAIN;
            case "GBTCLASSIFIER_TRAIN":
                return GBTCLASSIFIER_TRAIN;
            case "LINEARSVC_TRAIN":
                return LINEARSVC_TRAIN;
            case "LOGISTICREGRESSION_TRAIN":
                return LOGISTICREGRESSION_TRAIN;
            case "MULTILAYER_PERCEPTRON_CLASSIFIER_TRAIN":
                return MULTILAYER_PERCEPTRON_CLASSIFIER_TRAIN;
            case "NAIVE_BAYES_TRAIN":
                return NAIVE_BAYES_TRAIN;
            case "ONEVSREST_TRAIN":
                return ONEVSREST_TRAIN;
            case "RANDOM_FOREST_CLASSIFIER_TRAIN":
                return RANDOM_FOREST_CLASSIFIER_TRAIN;
            case "BISECTING_KMEANS_TRAIN":
                return BISECTING_KMEANS_TRAIN;
            case "GAUSSIAN_MIXTURE_TRAIN":
                return GAUSSIAN_MIXTURE_TRAIN;
            case "KMEANS_TRAIN":
                return KMEANS_TRAIN;
            case "LDA_TRAIN":
                return LDA_TRAIN;
            case "HASHINGTF_TRANSFORMER":
                return HASHINGTF_TRANSFORMER;
            case "IDF_TRANSFORMER":
                return IDF_TRANSFORMER;
            case "TOKENIZER_TRANSFORMER":
                return TOKENIZER_TRANSFORMER;
            case "COUNTVECTORIZER_TRANSFORMER":
                return COUNTVECTORIZER_TRANSFORMER;
            case "FEATUREHASHER_TRANSFORMER":
                return FEATUREHASHER_TRANSFORMER;
            case "WORD2VEC_TRANSFORMER":
                return WORD2VEC_TRANSFORMER;
            case "CHISQSELECTOR_TRANSFORMER":
                return CHISQSELECTOR_TRANSFORMER;
            case "RFORMULA_TRANSFORMER":
                return RFORMULA_TRANSFORMER;
            case "VECTORSLICER_TRANSFORMER":
                return VECTORSLICER_TRANSFORMER;
            case "BINARIZER_TRANSFORMER":
                return BINARIZER_TRANSFORMER;
            case "BUCKETIZER_TRANSFORMER":
                return BUCKETIZER_TRANSFORMER;
            case "DCT_TRANSFORMER":
                return DCT_TRANSFORMER;
            case "ELEMENT_WISE_PRODUCT_TRANSFORMER":
                return ELEMENT_WISE_PRODUCT_TRANSFORMER;
            case "IMPUTER_TRANSFORMER":
                return IMPUTER_TRANSFORMER;
            case "INDEX_TO_STRING_TRANSFORMER":
                return INDEX_TO_STRING_TRANSFORMER;
            case "MAXABS_SCALER_TRANSFORMER":
                return MAXABS_SCALER_TRANSFORMER;
            case "MINMAX_SCALER_TRANSFORMER":
                return MINMAX_SCALER_TRANSFORMER;
            case "NGRAM_TRANSFORMER":
                return NGRAM_TRANSFORMER;
            case "NORMALIZER_TRANSFORMER":
                return NORMALIZER_TRANSFORMER;
            case "ONEHOTENCODER_ESTIMATOR_TRANSFORMER":
                return ONEHOTENCODER_ESTIMATOR_TRANSFORMER;
            case "PCA_TRANSFORMER":
                return PCA_TRANSFORMER;
            case "POLYNOMIAL_EXPANSION_TRANSFORMER":
                return POLYNOMIAL_EXPANSION_TRANSFORMER;
            case "QUANTILE_DISCRETIZER_TRANSFORMER":
                return QUANTILE_DISCRETIZER_TRANSFORMER;
            case "STANDARD_SCALER_TRANSFORMER":
                return STANDARD_SCALER_TRANSFORMER;
            case "STOP_WORDS_REMOVER_TRANSFORMER":
                return STOP_WORDS_REMOVER_TRANSFORMER;
            case "STRING_INDEXER_TRANSFORMER":
                return STRING_INDEXER_TRANSFORMER;
            case "VECTOR_ASSEMBLER_TRANSFORMER":
                return VECTOR_ASSEMBLER_TRANSFORMER;
            case "VECTOR_INDEXER_TRANSFORMER":
                return VECTOR_INDEXER_TRANSFORMER;
            case "VECTOR_SIZE_HINT_TRANSFORMER":
                return VECTOR_SIZE_HINT_TRANSFORMER;
            case "DECISION_TREE_REGRESSION_TRAIN":
                return DECISION_TREE_REGRESSION_TRAIN;
            case "GBTREGRESSION_TRAIN":
                return GBTREGRESSION_TRAIN;
            case "ISOTONIC_REGRESSION_TRAIN":
                return ISOTONIC_REGRESSION_TRAIN;
            case "LINEAR_REGRESSION_TRAIN":
                return LINEAR_REGRESSION_TRAIN;
            case "RANDOM_FOREST_REGRESSION_TRAIN":
                return RANDOM_FOREST_REGRESSION_TRAIN;
            case "SURVIVAL_REGRESSION_TRAIN":
                return SURVIVAL_REGRESSION_TRAIN;
            case "ALS_TRAIN":
                return ALS_TRAIN;
            case "LOAD_EXTERNAL_MODEL":
                return LOAD_EXTERNAL_MODEL;
            case "FPGROWTH_TRAIN":
                return FPGROWTH_TRAIN;
            case "TRAIN_VALIDATION_SPLIT":
                return TRAIN_VALIDATION_SPLIT;
            case "DECISION_TREE_PREDICT":
                return DECISION_TREE_PREDICT;
            case "GBTCLASSIFIER_PREDICT":
                return GBTCLASSIFIER_PREDICT;
            case "LINEARSVC_PREDICT":
                return LINEARSVC_PREDICT;
            case "LOGISTICREGRESSION_PREDICT":
                return LOGISTICREGRESSION_PREDICT;
            case "MULTILAYER_PERCEPTRON_CLASSIFIER_PREDICT":
                return MULTILAYER_PERCEPTRON_CLASSIFIER_PREDICT;
            case "NAIVE_BAYES_PREDICT":
                return NAIVE_BAYES_PREDICT;
            case "ONEVSREST_PREDICT":
                return ONEVSREST_PREDICT;
            case "RANDOM_FOREST_CLASSIFIER_PREDICT":
                return RANDOM_FOREST_CLASSIFIER_PREDICT;
            case "BISECTING_KMEANS_PREDICT":
                return BISECTING_KMEANS_PREDICT;
            case "GAUSSIAN_MIXTURE_PREDICT":
                return GAUSSIAN_MIXTURE_PREDICT;
            case "KMEANS_PREDICT":
                return KMEANS_PREDICT;
            case "LDA_PREDICT":
                return LDA_PREDICT;
            case "HASHINGTF":
                return HASHINGTF;
            case "IDF":
                return IDF;
            case "TOKENIZER":
                return TOKENIZER;
            case "COUNTVECTORIZER":
                return COUNTVECTORIZER;
            case "FEATUREHASHER":
                return FEATUREHASHER;
            case "WORD2VEC":
                return WORD2VEC;
            case "CHISQSELECTOR":
                return CHISQSELECTOR;
            case "RFORMULA":
                return RFORMULA;
            case "VECTORSLICER":
                return VECTORSLICER;
            case "BINARIZER":
                return BINARIZER;
            case "BUCKETIZER":
                return BUCKETIZER;
            case "DCT":
                return DCT;
            case "ELEMENT_WISE_PRODUCT":
                return ELEMENT_WISE_PRODUCT;
            case "IMPUTER":
                return IMPUTER;
            case "INDEX_TO_STRING":
                return INDEX_TO_STRING;
            case "MAXABS_SCALER":
                return MAXABS_SCALER;
            case "MINMAX_SCALER":
                return MINMAX_SCALER;
            case "NGRAM":
                return NGRAM;
            case "NORMALIZER":
                return NORMALIZER;
            case "ONEHOTENCODER_ESTIMATOR":
                return ONEHOTENCODER_ESTIMATOR;
            case "PCA":
                return PCA;
            case "POLYNOMIAL_EXPANSION":
                return POLYNOMIAL_EXPANSION;
            case "QUANTILE_DISCRETIZER":
                return QUANTILE_DISCRETIZER;
            case "STANDARD_SCALER":
                return STANDARD_SCALER;
            case "STOP_WORDS_REMOVER":
                return STOP_WORDS_REMOVER;
            case "STRING_INDEXER":
                return STRING_INDEXER;
            case "VECTOR_ASSEMBLER":
                return VECTOR_ASSEMBLER;
            case "VECTOR_INDEXER":
                return VECTOR_INDEXER;
            case "VECTOR_SIZE_HINT":
                return VECTOR_SIZE_HINT;
            case "DECISION_TREE_REGRESSION_PREDICT":
                return DECISION_TREE_REGRESSION_PREDICT;
            case "GBTREGRESSION_PREDICT":
                return GBTREGRESSION_PREDICT;
            case "ISOTONIC_REGRESSION_PREDICT":
                return ISOTONIC_REGRESSION_PREDICT;
            case "LINEAR_REGRESSION_PREDICT":
                return LINEAR_REGRESSION_PREDICT;
            case "RANDOM_FOREST_REGRESSION_PREDICT":
                return RANDOM_FOREST_REGRESSION_PREDICT;
            case "SURVIVAL_REGRESSION_PREDICT":
                return SURVIVAL_REGRESSION_PREDICT;
            case "ALS_RECOMMEND":
                return ALS_RECOMMEND;
            case "ALS_PREDICT":
                return ALS_PREDICT;
            case "ALS_ITEM_USER_FACTORS":
                return ALS_ITEM_USER_FACTORS;
            case "PREDICT_BY_EXTERNAL_MODEL":
                return PREDICT_BY_EXTERNAL_MODEL;
            case "TRAIN_VALIDATION_PREDICT":
                return TRAIN_VALIDATION_PREDICT;
            case "PRECISIONBYTHRESHOLD":
                return PRECISIONBYTHRESHOLD;
            case "RECALLBYTHRESHOLD":
                return RECALLBYTHRESHOLD;
            case "REPARTITION":
                return REPARTITION;
            case "REPARTITION_BY_COUNT":
                return REPARTITION_BY_COUNT;
            case "FMEASUREBYTHRESHOLD":
                return FMEASUREBYTHRESHOLD;
            case "PR":
                return PR;
            case "ROC":
                return ROC;
            case "AUPRC":
                return AUPRC;
            case "AUROC":
                return AUROC;
            case "SILHOUETTESCORE":
                return SILHOUETTESCORE;
            case "FPGROWTH_FREQ":
                return FPGROWTH_FREQ;
            case "FPGROWTH_CONFIDENCE":
                return FPGROWTH_CONFIDENCE;
            case "GLR_STATISTIC":
                return GLR_STATISTIC;
            case "KMEANS_WSSSE":
                return KMEANS_WSSSE;
            case "BISECTING_KMEANS_WSSSE":
                return BISECTING_KMEANS_WSSSE;
            case "ACCURACY":
                return ACCURACY;
            case "WEIGHTEDFMEASURE":
                return WEIGHTEDFMEASURE;
            case "WEIGHTEDRECALL":
                return WEIGHTEDRECALL;
            case "WEIGHTEDPRECISION":
                return WEIGHTEDPRECISION;
            case "FPRBYLABEL":
                return FPRBYLABEL;
            case "WEIGHTEDFPR":
                return WEIGHTEDFPR;
            case "FMEASUREBYLABEL":
                return FMEASUREBYLABEL;
            case "PRECISIONBYLABEL":
                return PRECISIONBYLABEL;
            case "RECALLBYLABEL":
                return RECALLBYLABEL;
            case "CONFUSIONMATRIX":
                return CONFUSIONMATRIX;
            case "MLPRECISION":
                return MLPRECISION;
            case "MLPRECISIONBYLABEL":
                return MLPRECISIONBYLABEL;
            case "MLRECALL":
                return MLRECALL;
            case "MLRECALLBYLABEL":
                return MLRECALLBYLABEL;
            case "MLACCURACY":
                return MLACCURACY;
            case "MLFMEASURE":
                return MLFMEASURE;
            case "HAMMINGLOSS":
                return HAMMINGLOSS;
            case "MICROFMEASURE":
                return MICROFMEASURE;
            case "MICROPRECISION":
                return MICROPRECISION;
            case "MICRORECALL":
                return MICRORECALL;
            case "SUBSETACCURACY":
                return SUBSETACCURACY;
            case "MLFMEASUREBYLABEL":
                return MLFMEASUREBYLABEL;
            case "PRECISIONAT":
                return PRECISIONAT;
            case "MEANAVERAGEPRECISION":
                return MEANAVERAGEPRECISION;
            case "NDCGAT":
                return NDCGAT;
            case "MSE":
                return MSE;
            case "RMSE":
                return RMSE;
            case "R2":
                return R2;
            case "MAE":
                return MAE;
            case "GRAPH_BUILD":
                return GRAPH_BUILD;
            case "GRAPH_TRIANGLE_COUNT":
                return GRAPH_TRIANGLE_COUNT;
            case "GRAPH_STRONGLY_CONNECTED_COMPONENTS":
                return GRAPH_STRONGLY_CONNECTED_COMPONENTS;
            case "GRAPH_LABEL_PROPAGATION":
                return GRAPH_LABEL_PROPAGATION;
            case "GRAPH_N_DEGREE":
                return GRAPH_N_DEGREE;
            case "GRAPH_N_DEGREE_ALL":
                return GRAPH_N_DEGREE_ALL;
            case "GRAPH_BFS":
                return GRAPH_BFS;
            case "GRAPH_SHORTEST_PATHS":
                return GRAPH_SHORTEST_PATHS;
            case "GRAPH_PAGE_RANK":
                return GRAPH_PAGE_RANK;
            case "GRAPH_PAGE_RANK_PREGEL":
                return GRAPH_PAGE_RANK_PREGEL;
            case "GRAPH_CONNECTED_COMPONENTS":
                return GRAPH_CONNECTED_COMPONENTS;
            case "PIPELINE_BUILD":
                return PIPELINE_BUILD;
            case "PIPELINE_APPLY":
                return PIPELINE_APPLY;
            case "GLR_TRAIN":
                return GLR_TRAIN;
            case "GLR_PREDICT":
                return GLR_PREDICT;
            case "TENSORFLOW_MODEL_TRAIN":
                return TENSORFLOW_MODEL_TRAIN;
            case "TENSORFLOW_MODEL_PREDICT":
                return TENSORFLOW_MODEL_PREDICT;
            case "RETRIEVEBYSOURCESQL":
                return RETRIEVEBYSOURCESQL;
            default:
                throw new RuntimeException("can not convert ldb function " + name);
        }
    }

    public static SqlKind convertOpTypes(int opType) {

        switch (opType) {
            case 32:
                return ADD;
            case 33:
                return SUBTRACT;
            case 34:
                return MULTIPLY;
            case 35:
                return DIVIDE;
            case 36:
                return CONCAT;
            case 37:
                return LIKE_ARG;
            case 38:
                return CASEWHEN_COALESCE;
            case 39:
                return IS_NOT_NULL;
            case 40:
                return EQUAL;
            case 41:
                return GREATER_EQUAL;
            case 42:
                return GREATER_EQUAL_PRE;
            case 43:
                return GREATER;
            case 44:
                return SMALLER;
            case 45:
                return SMALLER_EQUAL;
            case 451:
                return SMALL_EQUAL_GREATER;
            case 46:
                return NOT_EQUAL;
            case 47:
                return IS_NULL;
            case 48:
                return NOT;
            case 49:
                return AND;
            case 50:
                return OR;
            case 51:
                return ALL_QUANTIFIED;
            case 52:
                return ANY_QUANTIFIED;
            case 53:
                return LIKE;
            case 54:
                return IN;
            case 55:
                return EXISTS;
            case 56:
                return RANGE_CONTAINS;
            case 57:
                return RANGE_EQUALS;
            case 58:
                return RANGE_OVERLAPS;
            case 59:
                return RANGE_PRECEDES;
            case 60:
                return RANGE_SUCCEEDS;
            case 61:
                return RANGE_IMMEDIATELY_PRECEDES;
            case 62:
                return RANGE_IMMEDIATELY_SUCCEEDS;
            case 63:
                return UNIQUE;
            case 64:
                return NOT_DISTINCT;
            case 65:
                return MATCH_SIMPLE;
            case 66:
                return MATCH_PARTIAL;
            case 67:
                return MATCH_FULL;
            case 68:
                return MATCH_UNIQUE_SIMPLE;
            case 69:
                return MATCH_UNIQUE_PARTIAL;
            case 70:
                return MATCH_UNIQUE_FULL;
            case 82:
                return ARRAY_AGG;
            case 83:
                return GROUP_CONCAT;
            case 84:
                return PREFIX;
            case 85:
                return MEDIAN;
            case 86:
                return CONCAT_WS;
            case 87:
                return CAST;
            case 88:
                return ZONE_MODIFIER;
            case 89:
                return CASEWHEN;
            case 90:
                return ORDER_BY;
            case 91:
                return LIMIT;
            case 92:
                return ALTERNATIVE;
            case 93:
                return MULTICOLUMN;
            case 94:
                return USER_AGGREGATE;
            case 95:
                return ARRAY_ACCESS;
            case 96:
                return ARRAY_SUBQUERY;
            case 97:
                return PREFIX;
            case 98:
                return GROUPING;
            case 200:
                return CURRENT_ROW;
            case 201:
                return UNBOUNDED_PRECEDING;
            case 202:
                return PRECEDING;
            case 203:
                return UNBOUNDED_FOLLOWING;
            case 204:
                return FOLLOWING;
            case 209:
                return GROUP_TO_ARRAY;

            default:
                throw new RuntimeException("can not convert optype " + opType);
        }
    }

    public static SqlKind convertAggregateOpTypes(int opType) {

        switch (opType) {
            case 71:
                return COUNT;
            case 72:
                return SUM;
            case 721:
                return MYAGG;
            case 722:
                return FIRST;
            case 723:
                return LAST;
            case 730:
                return NEXT;
            case 73:
                return MIN;
            case 74:
                return MAX;
            case 75:
                return AVG;
            case 76:
                return EVERY;
            case 77:
                return SOME;
            case 78:
                return STDDEV_POP;
            case 79:
                return STDDEV_SAMP;
            case 80:
                return VAR_POP;
            case 81:
                return VAR_SAMP;
            case 83:
                return GROUP_CONCAT;
            case 99:
                return RANK;
            case 100:
                return ROW_NUMBER;
            case 101:
                return DENSE_RANK;
            case 102:
                return PERCENT_RANK;
            case 103:
                return CUME_DIST;
            case 104:
                return NTILE;
            case 105:
                return LAG;
            case 106:
                return LEAD;
            case 107:
                return FIRST_VALUE;
            case 108:
                return LAST_VALUE;
            case 109:
                return KURTOSIS;
            case 110:
                return WINDOW;
            case 111:
                return WINDOW_FUNCTION;
            case 209:
                return GROUP_TO_ARRAY;
            default:
                throw new RuntimeException("can not convert aggregate func " + opType);
        }
    }

    public static SqlKind convertSqlFunctionSQL(int funcType) {
        switch (funcType) {
            case 1:
                return POSITION_CHAR;
            case 2:
                return POSITION_BINARY;
            case 3:
                return OCCURENCES_REGEX;
            case 4:
                return POSITION_REGEX;
            case 5:
                return EXTRACT;
            case 6:
                return BIT_LENGTH;
            case 7:
                return CHAR_LENGTH;
            case 8:
                return OCTET_LENGTH;
            case 9:
                return CARDINALITY;
            case 10:
                return MAX_CARDINALITY;
            case 11:
                return TRIM_ARRAY;
            case 12:
                return ABS;
            case 13:
                return MOD;
            case 14:
                return LN;
            case 15:
                return EXP;
            case 16:
                return POWER;
            case 17:
                return SQRT;
            case 20:
                return FLOOR;
            case 21:
                return CEILING;
            case 22:
                return WIDTH_BUCKET;
            case 23:
                return SUBSTRING_CHAR;
            case 24:
                return SUBSTRING_REG_EXPR;
            case 25:
                return SUBSTRING_REGEX;
            case 26:
                return FOLD_LOWER;
            case 27:
                return FOLD_UPPER;
            case 28:
                return TRANSCODING;
            case 29:
                return TRANSLITERATION;
            case 30:
                return REGEX_TRANSLITERATION;
            case 31:
                return TRIM_CHAR;
            case 32:
                return OVERLAY_CHAR;
            case 33:
                return CHAR_NORMALIZE;
            case 40:
                return SUBSTRING_BINARY;
            case 41:
                return TRIM_BINARY;
            case 42:
                return OVERLAY_BINARY;
            case 43:
                return CURRENT_DATE;
            case 44:
                return CURRENT_TIME;
            case 50:
                return CURRENT_TIMESTAMP;
            case 51:
                return LOCALTIME;
            case 52:
                return LOCALTIMESTAMP;
            case 63:
                return VALUE;
            case 64:
                return GROUPING;
            case 65:
                return NEW;
            case 67:
                return CONTAINS;
            case 70:
                return RECT_CONTAIN;
            case 71:
                return ACOS;
            case 72:
                return ACTION_ID;
            case 73:
                return ADD_MONTHS;
            case 74:
                return ASCII;
            case 75:
                return ASIN;
            case 76:
                return ATAN;
            case 77:
                return ATAN2;
            case 78:
                return BITAND;
            case 79:
                return BITANDNOT;
            case 80:
                return BITNOT;
            case 81:
                return BITOR;
            case 82:
                return BITXOR;
            case 83:
                return CHAR;
            case 84:
                return CONCAT;
            case 85:
                return COS;
            case 86:
                return COSH;
            case 87:
                return COT;
            case 88:
                return CRYPT_KEY;
            case 89:
                return DATABASE;
            case 90:
                return DATABASE_ISOLATION_LEVEL;
            case 91:
                return DATABASE_NAME;
            case 92:
                return DATABASE_TIMEZONE;
            case 93:
                return DATABASE_VERSION;
            case 94:
                return DATE_ADD;
            case 95:
                return DATE_SUB;
            case 96:
                return DATEADD;
            case 97:
                return DATEDIFF;
            case 98:
                return DAYS;
            case 99:
                return DBTIMEZONE;
            case 100:
                return DEGREES;
            case 101:
                return DIAGNOSTICS;
            case 102:
                return DIFFERENCE;
            case 103:
                return FROM_TZ;
            case 104:
                return HEXTORAW;
            case 105:
                return IDENTITY;
            case 106:
                return INSTR;
            case 107:
                return ISAUTOCOMMIT;
            case 108:
                return ISOLATION_LEVEL;
            case 109:
                return ISREADONLYDATABASE;
            case 110:
                return ISREADONLYDATABASEFILES;
            case 111:
                return ISREADONLYSESSION;
            case 112:
                return LAST_DAY;
            case 113:
                return LEFT;
            case 114:
                return LOAD_FILE;
            case 115:
                return LOB_ID;
            case 116:
                return LOCATE;
            case 117:
                return LOG10;
            case 118:
                return LPAD;
            case 119:
                return LTRIM;
            case 120:
                return MONTHS_BETWEEN;
            case 121:
                return NEW_TIME;
            case 122:
                return NEXT_DAY;
            case 123:
                return NUMTODSINTERVAL;
            case 124:
                return NUMTOYMINTERVAL;
            case 125:
                return PI;
            case 126:
                return POSITION_ARRAY;
            case 127:
                return RADIANS;
            case 128:
                return RAND;
            case 129:
                return RAWTOHEX;
            case 130:
                return REGEXP_MATCHES;
            case 131:
                return REGEXP_REPLACE;
            case 132:
                return REGEXP_SUBSTRING;
            case 133:
                return REGEXP_SUBSTRING_ARRAY;
            case 1034:
                return MYTEST;
            case 1035:
                return POW;
            case 1036:
                return FUNCTIONDESC;
            case 1037:
                return REBOOT;
            case 1038:
                return RESTARTSERVER;
            case 134:
                return REPEAT;
            case 135:
                return REPLACE;
            case 136:
                return REVERSE;
            case 137:
                return RIGHT;
            case 138:
                return ROUND;
            case 139:
                return ROUNDMAGIC;
            case 140:
                return RPAD;
            case 141:
                return RTRIM;
            case 142:
                return SECONDS_MIDNIGHT;
            case 143:
                return SEQUENCE_ARRAY;
            case 1431:
                return EXT_ARRAY;
            case 144:
                return SESSION_ID;
            case 145:
                return SESSION_ISOLATION_LEVEL;
            case 146:
                return SESSION_TIMEZONE;
            case 147:
                return SESSIONTIMEZONE;
            case 148:
                return SIGN;
            case 149:
                return SIN;
            case 150:
                return SINH;
            case 151:
                return SOUNDEX;
            case 152:
                return SORT_ARRAY;
            case 153:
                return SPACE;
            case 154:
                return SUBSTR;
            case 155:
                return SYS_EXTRACT_UTC;
            case 156:
                return SYSDATE;
            case 157:
                return SYSTIMESTAMP;
            case 158:
                return TAN;
            case 159:
                return TANH;
            case 160:
                return TIMESTAMP;
            case 161:
                return TIMESTAMP_WITH_ZONE;
            case 162:
                return TIMESTAMPADD;
            case 163:
                return TIMESTAMPDIFF;
            case 164:
                return TIMEZONE;
            case 165:
                return TO_CHAR;
            case 166:
                return TO_DATE;
            case 167:
                return TO_DSINTERVAL;
            case 168:
                return TO_YMINTERVAL;
            case 169:
                return TO_NUMBER;
            case 170:
                return TO_TIMESTAMP;
            case 171:
                return TO_TIMESTAMP_TZ;
            case 172:
                return TRANSACTION_CONTROL;
            case 173:
                return TRANSACTION_ID;
            case 174:
                return TRANSACTION_SIZE;
            case 175:
                return TRANSLATE;
            case 176:
                return TRUNC;
            case 177:
                return TRUNCATE;
            case 178:
                return UUID;
            case 179:
                return UNIX_TIMESTAMP;
            case 180:
                return UNIX_MILLIS;
            case 182:
                return DATEPART;
            case 183:
                return DATENAME;
            case 184:
                return NANVL;
            case 185:
                return SQLCODE;
            case 186:
                return SQLERRM;
            case 187:
                return STREAM_FUNC_DATE_ADD;
            case 188:
                return STREAM_FUNC_DATE_SUB;
            case 189:
                return STREAM_FUNC_CHAR;
            case 200:
                return RANK;
            case 201:
                return DENSE_RANK;
            case 202:
                return PERCENT_RANK;
            case 203:
                return CUME_DIST;
            case 204:
                return ROW_NUMBER;
            case 205:
                return NTILE;
            case 206:
                return LAG;
            case 207:
                return LEAD;
            case 208:
                return FIRST;
            case 209:
                return FIRST_VALUE;
            case 210:
                return LAST;
            case 211:
                return LAST_VALUE;
            case 212:
                return NTH_VALUE;
            case 213:
                return PREV;
            case 217:
                return JSON_TABLE;
            case 218:
                return FINAL;
            case 219:
                return RUNNING;
            case 220:
                return PATTERN_ALTER;
            case 224:
                return PATTERN_CONCAT;
            case 225:
                return PATTERN_EXCLUDE;
            case 226:
                return PATTERN_QUANTIFIER;
            case 227:
                return PATTERN_PERMUTE;
            case 228:
                return NEXT;
            default:
                throw new RuntimeException("can not convert function type " + funcType);
        }
    }

    public static SqlKind convertAggregateByName(String name) {
        switch (name) {
            case "APPROX_COUNT_DISTINCT":
                return APPROX_COUNT_DISTINCT;
            case "APPROX_PERCENTILE":
                return APPROX_PERCENTILE;
            case "AVG_WHERE":
                return AVG_WHERE;
            case "AVG_CATE_WHERE":
                return AVG_CATE_WHERE;
            case "CNT_CATE_WHERE":
                return CNT_CATE_WHERE;
            case "COUNT_WHERE":
                return COUNT_WHERE;
            case "CNT_OF_CATE":
                return CNT_OF_CATE;
            case "CONCAT_STR_LAST_K_WHERE":
                return CONCAT_STR_LAST_K_WHERE;
            case "CONCAT_STR_WHERE":
                return CONCAT_STR_WHERE;
            case "CORR":
                return CORR;
            case "COVAR_POP":
                return COVAR_POP;
            case "COVAR_SAMP":
                return COVAR_SAMP;
            case "KURTOSIS":
                return KURTOSIS;
            case "PERCENTILE_APPROX":
                return PERCENTILE_APPROX;
            case "REGR_SLOPE":
                return REGR_SLOPE;
            case "REGR_INTERCEPT":
                return REGR_INTERCEPT;
            case "REGR_COUNT":
                return REGR_COUNT;
            case "REGR_R2":
                return REGR_R2;
            case "REGR_SXX":
                return REGR_SXX;
            case "REGR_SYY":
                return REGR_SYY;
            case "REGR_AVGX":
                return REGR_AVGX;
            case "REGR_AVGY":
                return REGR_AVGY;
            case "REGR_SXY":
                return REGR_SXY;
            case "COLLECT_LIST":
                return COLLECT_LIST;
            case "COLLECT_SET":
                return COLLECT_SET;
            case "SKEWNESS":
                return SKEWNESS;
            case "STD":
                return STD;
            case "STDDEV":
                return STDDEV;
            case "SUM_WHERE":
                return SUM_WHERE;
            case "SUM_CATE_WHERE":
                return SUM_CATE_WHERE;
            case "TOPN_SUM_CATE_WHERE":
                return TOPN_SUM_CATE_WHERE;
            case "TOPN_CNT_CATE_WHERE":
                return TOPN_CNT_CATE_WHERE;
            case "TOPN_CATE_ORDERBY_SUM_WHERE":
                return TOPN_CATE_ORDERBY_SUM_WHERE;
            case "TOPN_CATE_ORDERBY_CNT_WHERE":
                return TOPN_CATE_ORDERBY_CNT_WHERE;
            case "LOG_CNT_CATE_WHERE":
                return LOG_CNT_CATE_WHERE;
            case "LOG_SUM_CATE_WHERE":
                return LOG_SUM_CATE_WHERE;
            case "MAX_N_WHERE":
                return MAX_N_WHERE;
            case "MIN_N_WHERE":
                return MIN_N_WHERE;
            case "VARIANCE":
                return VARIANCE;
            default:
                return UDAF;
        }
    }

    public static SqlKind convertByName(String name) {
        switch (name) {
            case "JSON_PATH":
                return JSON_PATH;
            case "TEST_EXECUTE":
                return TEST_EXECUTE;
            case "INPUT_FILE_NAME":
                return INPUT_FILE_NAME;
            case "DATE_TRUNC":
                return DATE_TRUNC;
            case "BINARY":
                return BINARY;
            case "BINARY_TO_DOUBLE_ARRAY":
                return BINARY_TO_DOUBLE_ARRAY;
            case "FORMAT_STRING":
                return FORMAT_STRING;
            case "ELT":
                return ELT;
            case "NAMED_STRUCT":
                return NAMED_STRUCT;
            case "JAVA_DECODE":
                return JAVA_DECODE;
            case "ISNULL":
                return ISNULL;
            case "REGEXP_EXTRACT":
                return REGEXP_EXTRACT;
            case "ARRAY_CONTAINS":
                return ARRAY_CONTAINS;
            case "ARRAY_POSITION":
                return ARRAY_POSITION;
            case "XPATH":
                return XPATH;
            case "XPATH_BOOLEAN":
                return XPATH_BOOLEAN;
            case "XPATH_DOUBLE":
                return XPATH_DOUBLE;
            case "XPATH_FLOAT":
                return XPATH_FLOAT;
            case "XPATH_INT":
                return XPATH_INT;
            case "XPATH_LONG":
                return XPATH_LONG;
            case "XPATH_NUMBER":
                return XPATH_NUMBER;
            case "XPATH_SHORT":
                return XPATH_SHORT;
            case "XPATH_STRING":
                return XPATH_STRING;
            case "ASSERT_TRUE":
                return ASSERT_TRUE;
            case "BASE64":
                return BASE64;
            case "BIGINT":
                return BIGINT;
            case "BIN":
                return BIN;
            case "BOOLEAN":
                return BOOLEAN;
            case "BROUND":
                return BROUND;
            case "CBRT":
                return CBRT;
            case "CIRCLE_CONTAIN":
                return CIRCLE_CONTAIN;
            case "DISTANCE_WITHIN":
                return DISTANCE_WITHIN;
            case "CONV":
                return CONV;
            case "COUNT_MIN_SKETCH":
                return COUNT_MIN_SKETCH;
            case "CRC32":
                return CRC32;
            case "CUBE":
                return CUBE;
            case "CURRENT_DATABASE":
                return CURRENT_DATABASE;
            case "CVTCOLOR":
                return CVTCOLOR;
            case "DATE_FORMAT":
                return DATE_FORMAT;
            case "DAY":
                return DAY;
            case "DECIMAL":
                return DECIMAL;
            case "DOUBLE":
                return DOUBLE;
            case "E":
                return E;
            case "ENCODE":
                return ENCODE;
            case "ENCODE_IMAGE":
                return ENCODE_IMAGE;
            case "EXPLODE":
                return EXPLODE;
            case "EXPLODE_OUTER":
                return EXPLODE_OUTER;
            case "EXPM1":
                return EXPM1;
            case "FACTORIAL":
                return FACTORIAL;
            case "FIND_IN_SET":
                return FIND_IN_SET;
            case "FILE_TO_BINARY":
                return FILE_TO_BINARY;
            case "FLOAT":
                return FLOAT;
            case "FORMAT_NUMBER":
                return FORMAT_NUMBER;
            case "FROM_JSON":
                return FROM_JSON;
            case "FROM_UNIXTIME":
                return FROM_UNIXTIME;
            case "FROM_UTC_TIMESTAMP":
                return FROM_UTC_TIMESTAMP;
            case "GET_JSON_OBJECT":
                return GET_JSON_OBJECT;
            case "GROUPING_ID":
                return GROUPING_ID;
            case "HASH":
                return HASH;
            case "HEX":
                return HEX;
            case "HOUR":
                return HOUR;
            case "HYPOT":
                return HYPOT;
            case "IF":
                return IF;
            case "INITCAP":
                return INITCAP;
            case "INLINE":
                return INLINE;
            case "INLINE_OUTER":
                return INLINE_OUTER;
            case "INPUT_FILE_BLOCK_LENGTH":
                return INPUT_FILE_BLOCK_LENGTH;
            case "INPUT_FILE_BLOCK_START":
                return INPUT_FILE_BLOCK_START;
            case "INT":
                return INT;
            case "ISNAN":
                return ISNAN;
            case "ISNOTNULL":
                return ISNOTNULL;
            case "JAVA_METHOD":
                return JAVA_METHOD;
            case "JSON_TUPLE":
                return JSON_TUPLE;
            case "LEVENSHTEIN":
                return LEVENSHTEIN;
            case "LOG1P":
                return LOG1P;
            case "LOG2":
                return LOG2;
            case "MAP":
                return MAP;
            case "MAP_KEYS":
                return MAP_KEYS;
            case "MAP_VALUES":
                return MAP_VALUES;
            case "MD5":
                return MD5;
            case "MEAN":
                return MEAN;
            case "MONOTONICALLY_INCREASING_ID":
                return MONOTONICALLY_INCREASING_ID;
            case "NEGATIVE":
                return NEGATIVE;
            case "PARSE_URL":
                return PARSE_URL;
            case "PERCENTILE":
                return PERCENTILE;
            case "PMOD":
                return PMOD;
            case "POLYGON_CONTAIN":
                return POLYGON_CONTAIN;
            case "POSEXPLODE":
                return POSEXPLODE;
            case "POSEXPLODE_OUTER":
                return POSEXPLODE_OUTER;
            case "POSITIVE":
                return POSITIVE;
            case "PRINTF":
                return PRINTF;
            case "QUARTER":
                return QUARTER;
            case "RANDN":
                return RANDN;
            case "REFLECT":
                return REFLECT;
            case "RESIZE":
                return RESIZE;
            case "RINT":
                return RINT;
            case "RLIKE":
                return RLIKE;
            case "SENTENCES":
                return SENTENCES;
            case "SHA":
                return SHA;
            case "SHA1":
                return SHA1;
            case "SHA2":
                return SHA2;
            case "SHIFTLEFT":
                return SHIFTLEFT;
            case "SHIFTRIGHT":
                return SHIFTRIGHT;
            case "SHIFTRIGHTUNSIGNED":
                return SHIFTRIGHTUNSIGNED;
            case "SIGNUM":
                return SIGNUM;
            case "SIZE":
                return SIZE;
            case "SMALLINT":
                return SMALLINT;
            case "SPARK_PARTITION_ID":
                return SPARK_PARTITION_ID;
            case "SPLIT":
                return SPLIT;
            case "STACK":
                return STACK;
            case "STR_TO_MAP":
                return STR_TO_MAP;
            case "STRING":
                return STRING;
            case "STRUCT":
                return STRUCT;
            case "SUBSTRING_INDEX":
                return SUBSTRING_INDEX;
            case "TINYINT":
                return TINYINT;
            case "TO_JSON":
                return TO_JSON;
            case "TO_UNIX_TIMESTAMP":
                return TO_UNIX_TIMESTAMP;
            case "TO_UTC_TIMESTAMP":
                return TO_UTC_TIMESTAMP;
            case "UNBASE64":
                return UNBASE64;
            case "UNHEX":
                return UNHEX;
            case "WEEKOFYEAR":
                return WEEKOFYEAR;
            case "YEAR":
                return YEAR;
            case "VECTOR":
                return VECTOR;
            case "TUMBLE_START":
                return TUMBLE_START;
            case "TUMBLE_END":
                return TUMBLE_END;
            case "TUMBLE_ROWTIME":
                return TUMBLE_ROWTIME;
            case "TUMBLE_PROCTIME":
                return TUMBLE_PROCTIME;
            case "TUMBLE":
                return TUMBLE;
            case "HOP_START":
                return HOP_START;
            case "HOP_END":
                return HOP_END;
            case "HOP_ROWTIME":
                return HOP_ROWTIME;
            case "HOP_PROCTIME":
                return HOP_PROCTIME;
            case "HOP":
                return HOP;
            case "SESSION_START":
                return SESSION_START;
            case "SESSION_END":
                return SESSION_END;
            case "SESSION_ROWTIME":
                return SESSION_ROWTIME;
            case "SESSION_PROCTIME":
                return SESSION_PROCTIME;
            case "SESSION":
                return SESSION;
            case "SYS_STREAM_SNAPSHOT":
                return SYS_STREAM_SNAPSHOT;
            case "SYS_STREAM_RETRIEVE_RESULT":
                return SYS_STREAM_RETRIEVE_RESULT;
            case "STRUCT_TO_JSON":
                return STRUCT_TO_JSON;
            case "JSON_TO_STRUCT":
                return JSON_TO_STRUCT;
            case "GEO_DECODE":
                return GEO_DECODE;
            case "ARRAY_TO_STRING":
                return ARRAY_TO_STRING;
            case "GEO_ENCODE":
                return GEO_ENCODE;
            case "PYTHONUDF":
                return PYTHONUDF;
            // for CEP MATCH_RECOGNIZE
            case "FINAL":
                return FINAL;
            case "RUNNING":
                return RUNNING;
            case "PATTERN_ALTER":
                return PATTERN_ALTER;
            case "PATTERN_CONCAT":
                return PATTERN_CONCAT;
            case "PATTERN_EXCLUDE":
                return PATTERN_EXCLUDE;
            case "PATTERN_PERMUTE":
                return PATTERN_PERMUTE;
            case "PATTERN_QUANTIFIER":
                return PATTERN_QUANTIFIER;
            case "NEXT":
                return NEXT;
            default:
                return UDF;
        }
    }

    public static Boolean checkDeterministic(SqlKind sqlKind) {
        switch (sqlKind) {
            case RAND:
            case RANDN:
            case REFLECT:
            case COLLECT_LIST:
            case COLLECT_SET:
                return true;
            default:
                return null;
        }
    }

    public static Boolean checkImageFunction(SqlKind sqlKind) {
        switch (sqlKind) {
            case CVTCOLOR:
            case RESIZE:
            case ENCODE_IMAGE:
                return true;
            default:
                return false;
        }
    }

    public static boolean isInSqlKind(String typeString) {
        for (SqlKind type : SqlKind.values()) {
            if (type.name().equalsIgnoreCase(typeString)) {
                return true;
            }
        }
        return false;
    }
}
