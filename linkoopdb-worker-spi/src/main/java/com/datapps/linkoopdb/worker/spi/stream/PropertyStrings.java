/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datapps.linkoopdb.worker.spi.stream;

/**
 * Strings used for key and values in an environment file.
 */
public final class PropertyStrings {

    public static final String EXECUTION = "execution";
    public static final String EXECUTION_TYPE = "type";
    public static final String EXECUTION_TYPE_VALUE_STREAMING = "streaming";
    public static final String EXECUTION_TYPE_VALUE_BATCH = "batch";
    public static final String EXECUTION_TIME_CHARACTERISTIC = "time-characteristic";
    public static final String EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME = "event-time";
    public static final String EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME = "processing-time";
    public static final String EXECUTION_PERIODIC_WATERMARKS_INTERVAL = "periodic-watermarks-interval";
    public static final String EXECUTION_MIN_STATE_RETENTION = "min-idle-state-retention";
    public static final String EXECUTION_MAX_STATE_RETENTION = "max-idle-state-retention";
    public static final String EXECUTION_STREAM_QUERY_LATENCY_MILLS = "query-latency-mills";
    public static final String EXECUTION_STREAM_QUERY_LATENCY_COUNT = "query-latency-count";
    public static final String EXECUTION_MAX_QUERY_LATENCY_COUNT = "max-query-latency-count";
    public static final String EXECUTION_QUERY_MODE = "query-mode";
    public static final String EXECUTION_QUERY_MODE_LDB = "batch";
    public static final String EXECUTION_QUERY_MODE_ISTREAM = "stream";
    public static final String EXECUTION_QUERY_MODE_AUTO = "auto";
    public static final String EXECUTION_PARALLELISM = "parallelism";
    public static final String EXECUTION_MAX_PARALLELISM = "max-parallelism";
    public static final String EXECUTION_BUFFER_TIMEOUT = "buffer-timeout";
    public static final String EXECUTION_RESULT_MODE = "result-mode";
    public static final String EXECUTION_RESULT_MODE_VALUE_CHANGELOG = "changelog";
    public static final String EXECUTION_RESULT_MODE_VALUE_TABLE = "table";
    public static final String EXECUTION_RESTART_STRATEGY_TYPE = "restart-strategy.type";
    public static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FALLBACK = "fallback";
    public static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_NONE = "none";
    public static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FIXED_DELAY = "fixed-delay";
    public static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FAILURE_RATE = "failure-rate";
    public static final String EXECUTION_RESTART_STRATEGY_ATTEMPTS = "restart-strategy.attempts";
    public static final String EXECUTION_RESTART_STRATEGY_DELAY = "restart-strategy.delay";
    public static final String EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL = "restart-strategy.failure-rate-interval";
    public static final String EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL = "restart-strategy.max-failures-per-interval";
    public static final String EXECUTION_CHECKPOINT_ENABLED = "checkpoint.enabled";
    public static final String EXECUTION_CHECKPOINT_ENABLED_VALUE_TRUE = "true";
    public static final String EXECUTION_CHECKPOINT_MODE = "checkpoint.mode";
    public static final String EXECUTION_CHECKPOINT_MODE_VALUE_EXACTLY_ONCE = "exactly_once";
    public static final String EXECUTION_CHECKPOINT_MODE_VALUE_AT_LEAST_ONCE = "at_least_once";
    public static final String EXECUTION_CHECKPOINT_INTERVAL = "checkpoint.interval";
    public static final String EXECUTION_QUERY_SQL_ENABLED = "query.sql.enabled";
    public static final String DEPLOYMENT = "deployment";
    public static final String DEPLOYMENT_TYPE = "type";
    public static final String DEPLOYMENT_TYPE_VALUE_STANDALONE = "standalone";
    public static final String DEPLOYMENT_RESPONSE_TIMEOUT = "response-timeout";
    public static final String DEPLOYMENT_GATEWAY_ADDRESS = "gateway-address";
    public static final String DEPLOYMENT_GATEWAY_PORT = "gateway-port";
    public static final String EXECUTION_MACHINE_LEARNING = "ml";
    public static final String EXECUTION_MACHINE_LEARNING_BATCH_SIZE = "batch-size";
    public static final String EXECUTION_MACHINE_LEARNING_WAIT_DELAY = "wait-delay";
    public static final String EXECUTION_INTERNAL_SOURCE = "internal-source";
    public static final String EXECUTION_INTERNAL_SINK = "internal-sink";
    public static final String EXECUTION_SOURCE_FETCH_SIZE = "fetch-size";
    public static final String EXECUTION_SINK_BATCH_SIZE = "batch-size";
    public static final String EXECUTION_LDB_SERVER_URL = "ldb-server-url";
    public static final String EXECUTION_LDB_SERVER_TOKEN = "ldb-server-token";

    private PropertyStrings() {
        // private
    }

    public static String getExecutionMachineLearningName() {
        return EXECUTION + "." + EXECUTION_MACHINE_LEARNING;
    }

    public static String getExecutionMLBatchSizeName() {
        return EXECUTION + "." + EXECUTION_MACHINE_LEARNING + "." + EXECUTION_MACHINE_LEARNING_BATCH_SIZE;
    }

    public static String getExecutionMLWaitDelayName() {
        return EXECUTION + "." + EXECUTION_MACHINE_LEARNING + "." + EXECUTION_MACHINE_LEARNING_WAIT_DELAY;
    }

    public static String concat(String... names) {
        if (names == null || names.length < 1) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < names.length; index++) {
            builder.append(names[index])
                .append(index == names.length - 1 ? "" : ".");
        }

        return builder.toString();
    }
}
