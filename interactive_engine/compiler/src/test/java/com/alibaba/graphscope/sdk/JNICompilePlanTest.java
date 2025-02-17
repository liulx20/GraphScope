/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.sdk;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class JNICompilePlanTest {
    private static String configPath;
    private static String schemaYaml;
    private static String statsJson;

    @BeforeClass
    public static void before() throws Exception {
        configPath = "src/test/resources/config/gs_interactive_hiactor.yaml";
        schemaYaml =
                FileUtils.readFileToString(
                        new File("src/test/resources/schema/sls_schema.yaml"),
                        StandardCharsets.UTF_8);
        statsJson =
                FileUtils.readFileToString(
                        new File("src/test/resources/statistics/sls_statistics.json"),
                        StandardCharsets.UTF_8);
    }

    @Test
    public void path_expand_test() throws Exception {
        String query =
                "MATCH (src)-[e:test6*4..5]->(dest) WHERE src.__domain__ = 'xzz' RETURN"
                        + " src.__entity_id__ AS sId, dest.__entity_id__ AS dId;";
        PlanUtils.compilePlan(configPath, query, schemaYaml, statsJson);
    }

    @Test
    public void path_expand_max_hop_test() throws Exception {
        try {
            String query = "MATCH (src)-[e:test6*1..1000000]->(dest) Return src, dest";
            PlanUtils.compilePlan(configPath, query, schemaYaml, statsJson);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("exceeds the maximum allowed iterations"));
        }
    }

    @Test
    public void path_expand_invalid_hop_test() throws Exception {
        try {
            // the max hop will be set as unlimited if it is less than min hop
            String query = "MATCH (src)-[e:test6*5..4]->(dest) Return src, dest";
            PlanUtils.compilePlan(configPath, query, schemaYaml, statsJson);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("exceeds the maximum allowed iterations"));
        }
    }
}
