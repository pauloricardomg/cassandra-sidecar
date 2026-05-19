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

package org.apache.cassandra.sidecar.configmanagement;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.json.JsonObject;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ConfigUtils}
 */
class ConfigUtilsTest
{
    @TempDir
    Path tempDir;

    @Test
    void testMergeOverlayWins()
    {
        JsonObject base = new JsonObject()
                          .put("concurrent_reads", 32)
                          .put("cluster_name", "original");

        JsonObject overlay = new JsonObject()
                             .put("concurrent_reads", 64);

        JsonObject result = ConfigUtils.mergeConfigurations(base, overlay);

        assertThat(result.getInteger("concurrent_reads")).isEqualTo(64);
        assertThat(result.getString("cluster_name")).isEqualTo("original");
    }

    @Test
    void testMergeAddsNewKeys()
    {
        JsonObject base = new JsonObject()
                          .put("cluster_name", "test");

        JsonObject overlay = new JsonObject()
                             .put("concurrent_reads", 64)
                             .put("storage_compatibility_mode", "CASSANDRA_5");

        JsonObject result = ConfigUtils.mergeConfigurations(base, overlay);

        assertThat(result.getString("cluster_name")).isEqualTo("test");
        assertThat(result.getInteger("concurrent_reads")).isEqualTo(64);
        assertThat(result.getString("storage_compatibility_mode")).isEqualTo("CASSANDRA_5");
    }

    @Test
    void testMergeDeepNestedValues()
    {
        JsonObject skiplist = new JsonObject().put("class_name", "SkipListMemtable");
        JsonObject defaultConfig = new JsonObject().put("inherits", "skiplist");
        JsonObject configurations = new JsonObject()
                                    .put("skiplist", skiplist.getMap())
                                    .put("default", defaultConfig.getMap());
        JsonObject memtable = new JsonObject().put("configurations", configurations.getMap());
        JsonObject base = new JsonObject()
                          .put("memtable", memtable.getMap())
                          .put("cluster_name", "test");

        JsonObject trie = new JsonObject().put("class_name", "TrieMemtable");
        JsonObject overlayDefault = new JsonObject().put("inherits", "trie");
        JsonObject overlayConfigurations = new JsonObject()
                                           .put("trie", trie.getMap())
                                           .put("default", overlayDefault.getMap());
        JsonObject overlayMemtable = new JsonObject().put("configurations", overlayConfigurations.getMap());
        JsonObject overlay = new JsonObject().put("memtable", overlayMemtable.getMap());

        JsonObject result = ConfigUtils.mergeConfigurations(base, overlay);

        // Deep-merged: skiplist preserved from base, trie added from overlay, default overridden
        JsonObject resultConfigs = result.getJsonObject("memtable").getJsonObject("configurations");
        assertThat(resultConfigs.getJsonObject("skiplist").getString("class_name")).isEqualTo("SkipListMemtable");
        assertThat(resultConfigs.getJsonObject("trie").getString("class_name")).isEqualTo("TrieMemtable");
        assertThat(resultConfigs.getJsonObject("default").getString("inherits")).isEqualTo("trie");
        assertThat(result.getString("cluster_name")).isEqualTo("test");
    }

    @Test
    void testMergeEmptyOverlay()
    {
        JsonObject base = new JsonObject()
                          .put("cluster_name", "test")
                          .put("concurrent_reads", 32);

        JsonObject overlay = new JsonObject();

        JsonObject result = ConfigUtils.mergeConfigurations(base, overlay);

        assertThat(result.getString("cluster_name")).isEqualTo("test");
        assertThat(result.getInteger("concurrent_reads")).isEqualTo(32);
    }

    @Test
    void testFormatJvmOptsSystemProperty()
    {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("-Dcassandra.ring_delay_ms", "60000");
        opts.put("-Dcassandra.jmx.local.port", "7199");

        List<String> lines = ConfigUtils.formatJvmOptions(opts);

        assertThat(lines).containsExactly(
                "-Dcassandra.ring_delay_ms=60000",
                "-Dcassandra.jmx.local.port=7199"
        );
    }

    @Test
    void testFormatJvmOptsNonStandard()
    {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("-Xmx", "4g");
        opts.put("-Xms", "512m");
        opts.put("-Xss", "256k");

        List<String> lines = ConfigUtils.formatJvmOptions(opts);

        assertThat(lines).containsExactly("-Xmx4g", "-Xms512m", "-Xss256k");
    }

    @Test
    void testFormatJvmOptsAdvancedValue()
    {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("-XX:ParallelGCThreads", "8");
        opts.put("-XX:MaxGCPauseMillis", "200");

        List<String> lines = ConfigUtils.formatJvmOptions(opts);

        assertThat(lines).containsExactly(
                "-XX:ParallelGCThreads=8",
                "-XX:MaxGCPauseMillis=200"
        );
    }

    @Test
    void testFormatJvmOptsBooleanFlag()
    {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("-XX:+PrintGCDetails", "");
        opts.put("-XX:-UseCompressedOops", null);

        List<String> lines = ConfigUtils.formatJvmOptions(opts);

        assertThat(lines).containsExactly(
                "-XX:+PrintGCDetails",
                "-XX:-UseCompressedOops"
        );
    }

    @Test
    void testLoadYamlRealCassandraConfig()
    {
        Path yamlPath = Paths.get("src/test/resources/configmanagement/cassandra_latest.yaml");
        JsonObject config = ConfigUtils.loadYaml(yamlPath);

        assertThat(config).isNotNull();
        assertThat(config.getString("cluster_name")).isEqualTo("Test Cluster");
        assertThat(config.getInteger("num_tokens")).isEqualTo(16);
        assertThat(config.getString("commitlog_sync")).isEqualTo("periodic");
        // Verify nested structure
        assertThat(new JsonObject(config.getJsonObject("memtable").getMap())
                .getJsonObject("configurations")
                .getJsonObject("trie")
                .getString("class_name"))
                .isEqualTo("TrieMemtable");
        assertThat(config.containsKey("seed_provider")).isTrue();
    }

    @Test
    void testAtomicWriteNoPartialFiles() throws IOException
    {
        Path target = tempDir.resolve("test-output.txt");
        ConfigUtils.atomicWrite(target, path -> Files.write(path, List.of("hello", "world"), StandardCharsets.UTF_8));

        assertThat(target).exists();
        assertThat(Files.readString(target, StandardCharsets.UTF_8).trim()).isEqualTo("hello\nworld");

        try (Stream<Path> files = Files.list(tempDir))
        {
            List<String> fileNames = files.map(p -> p.getFileName().toString())
                                          .sorted()
                                          .collect(Collectors.toList());
            assertThat(fileNames).containsExactly("test-output.txt");
        }
    }
}
