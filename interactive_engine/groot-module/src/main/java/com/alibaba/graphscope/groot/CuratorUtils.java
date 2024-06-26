/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot;

import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.ZkConfig;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateErrorPolicy;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class CuratorUtils {
    private static final Logger logger = LoggerFactory.getLogger(CuratorUtils.class);

    public static CuratorFramework makeCurator(Configs configs) {
        String connectionString = ZkConfig.ZK_CONNECT_STRING.get(configs);
        int sessionTimeoutMs = ZkConfig.ZK_SESSION_TIMEOUT_MS.get(configs);
        int connectionTimeoutMs = ZkConfig.ZK_CONNECTION_TIMEOUT_MS.get(configs);
        int baseSleepMs = ZkConfig.ZK_BASE_SLEEP_MS.get(configs);
        int maxSleepMs = ZkConfig.ZK_MAX_SLEEP_MS.get(configs);
        int maxRetry = ZkConfig.ZK_MAX_RETRY.get(configs);
        boolean authEnable = ZkConfig.ZK_AUTH_ENABLE.get(configs);
        String authUser = ZkConfig.ZK_AUTH_USER.get(configs);
        String authPassword = ZkConfig.ZK_AUTH_PASSWORD.get(configs);

        BoundedExponentialBackoffRetry retryPolicy =
                new BoundedExponentialBackoffRetry(baseSleepMs, maxSleepMs, maxRetry);
        ConnectionStateErrorPolicy errorPolicy = state -> state == ConnectionState.LOST;
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectString(connectionString)
                .retryPolicy(retryPolicy)
                .connectionStateErrorPolicy(errorPolicy)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs);
        if (authEnable) {
            builder.authorization(
                            "digest",
                            (authUser + ":" + authPassword).getBytes(StandardCharsets.UTF_8))
                    .aclProvider(
                            new ACLProvider() {
                                @Override
                                public List<ACL> getDefaultAcl() {
                                    return ZooDefs.Ids.CREATOR_ALL_ACL;
                                }

                                @Override
                                public List<ACL> getAclForPath(String s) {
                                    return ZooDefs.Ids.CREATOR_ALL_ACL;
                                }
                            });
        }
        return builder.build();
    }
}
