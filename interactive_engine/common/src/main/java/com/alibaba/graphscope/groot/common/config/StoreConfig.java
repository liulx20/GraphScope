/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.common.config;

public class StoreConfig {
    public static final Config<String> STORE_DATA_PATH =
            Config.stringConfig("store.data.path", "./data");

    public static final Config<String> STORE_DATA_DOWNLOAD_PATH =
            Config.stringConfig("store.data.download.path", "");

    public static final Config<Integer> STORE_WRITE_THREAD_COUNT =
            Config.intConfig("store.write.thread.count", 1);

    public static final Config<Integer> STORE_QUEUE_BUFFER_SIZE =
            Config.intConfig("store.queue.buffer.size", 1024000);

    public static final Config<Long> STORE_QUEUE_WAIT_MS =
            Config.longConfig("store.queue.wait.ms", 3000L);

    public static final Config<Boolean> STORE_GC_ENABLE =
            Config.boolConfig("store.gc.enable", true);

    public static final Config<Long> STORE_GC_INTERVAL_MS =
            Config.longConfig("store.gc.interval.ms", 3600000L);

    public static final Config<Long> STORE_CATCHUP_INTERVAL_MS =
            Config.longConfig("store.catchup.interval.ms", 5000L);

    // set by IS_SECONDARY_INSTANCE, used in graph.rs
    public static final Config<String> STORE_STORAGE_ENGINE =
            Config.stringConfig("store.storage.engine", "rocksdb");
    public static final Config<String> STORE_SECONDARY_DATA_PATH =
            Config.stringConfig("store.data.secondary.path", "./data_secondary");

    public static final Config<String> STORE_WAL_DIR =
            Config.stringConfig("store.rocksdb.wal.dir", "");

    public static final Config<Integer> STORE_COMPACT_THREAD_NUM =
            Config.intConfig("store.compact.thread.num", 1);
}
