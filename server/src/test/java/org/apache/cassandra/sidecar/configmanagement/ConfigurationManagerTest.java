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
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ConfigurationManager}
 */
class ConfigurationManagerTest
{
    private static final Path BASE_TEMPLATE = Paths.get("src/test/resources/configmanagement/cassandra_latest.yaml");

    @TempDir
    Path configStore;

    private InMemoryConfigurationProvider provider;

    @BeforeEach
    void setUp()
    {
        provider = new InMemoryConfigurationProvider();
    }

    @Test
    void testInitializeWithNoOverlay()
    {
        ConfigurationManager manager = new ConfigurationManager(provider, configStore, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);

        manager.initialize(List.of(instance));

        Path instanceDir = configStore.resolve("1");
        assertThat(instanceDir.resolve("cassandra.yaml")).isRegularFile();
        assertThat(instanceDir.resolve("extra-jvm.options")).isRegularFile();

        // cassandra.yaml should be the base template content
        JsonObject materialized = ConfigUtils.loadYaml(instanceDir.resolve("cassandra.yaml"));
        assertThat(materialized.getString("cluster_name")).isEqualTo("Test Cluster");
        assertThat(materialized.getInteger("num_tokens")).isEqualTo(16);

        // extra-jvm.options should be empty
        assertThat(instanceDir.resolve("extra-jvm.options")).hasContent("");

        // overlay_cache.json should not exist since provider returned null
        assertThat(instanceDir.resolve("overlay_cache.json")).doesNotExist();
    }

    @Test
    void testInitializeWithOverlay() throws IOException
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject yamlOverlay = new JsonObject()
                                 .put("concurrent_reads", 128)
                                 .put("memtable_flush_writers", 8);

        Map<String, String> jvmOpts = new LinkedHashMap<>();
        jvmOpts.put("-Xmx", "4g");
        jvmOpts.put("-Dcassandra.ring_delay_ms", "60000");

        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yamlOverlay, jvmOpts);
        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(Instant.now(), overlay);
        provider.storeOverlay(instance, null, snapshot);

        ConfigurationManager manager = new ConfigurationManager(provider, configStore, BASE_TEMPLATE);
        manager.initialize(List.of(instance));

        Path instanceDir = configStore.resolve("1");

        // Verify cassandra.yaml has merged values
        JsonObject materialized = ConfigUtils.loadYaml(instanceDir.resolve("cassandra.yaml"));
        assertThat(materialized.getInteger("concurrent_reads")).isEqualTo(128);
        assertThat(materialized.getInteger("memtable_flush_writers")).isEqualTo(8);
        // Base template values preserved
        assertThat(materialized.getString("cluster_name")).isEqualTo("Test Cluster");
        assertThat(materialized.getInteger("num_tokens")).isEqualTo(16);

        // Verify extra-jvm.options
        List<String> jvmLines = Files.readAllLines(instanceDir.resolve("extra-jvm.options"), StandardCharsets.UTF_8);
        assertThat(jvmLines).containsExactly("-Xmx4g", "-Dcassandra.ring_delay_ms=60000");

        // Verify overlay_cache.json cached
        assertThat(instanceDir.resolve("overlay_cache.json")).isRegularFile();
        String content = Files.readString(instanceDir.resolve("overlay_cache.json"), StandardCharsets.UTF_8);
        ConfigurationOverlaySnapshot cached = ConfigurationOverlaySnapshot.fromJson(new JsonObject(content));
        assertThat(cached.overlay().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(128);
        assertThat(cached.overlay().extraJvmOpts()).containsEntry("-Xmx", "4g");
    }

    @Test
    void testProviderUnavailable()
    {
        ConfigurationProvider failingProvider = new ConfigurationProvider()
        {
            @Override
            public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata instance)
            {
                throw new UncheckedIOException(new IOException("provider unavailable"));
            }

            @Override
            public boolean storeOverlay(InstanceMetadata instance, String originalHash,
                                        ConfigurationOverlaySnapshot newSnapshot)
            {
                throw new UnsupportedOperationException();
            }
        };

        ConfigurationManager manager = new ConfigurationManager(failingProvider, configStore, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);

        manager.initialize(List.of(instance));

        // No files should be materialized when provider is unavailable
        Path instanceDir = configStore.resolve("1");
        assertThat(instanceDir).doesNotExist();
    }

    @Test
    void testMultipleInstances()
    {
        InstanceMetadata instance1 = mockInstance(1);
        InstanceMetadata instance2 = mockInstance(2);

        JsonObject overlay1Yaml = new JsonObject().put("concurrent_reads", 64);
        CassandraConfigurationOverlay overlay1 = new CassandraConfigurationOverlay(overlay1Yaml, null);
        provider.storeOverlay(instance1, null, new ConfigurationOverlaySnapshot(Instant.now(), overlay1));

        JsonObject overlay2Yaml = new JsonObject().put("concurrent_reads", 128);
        CassandraConfigurationOverlay overlay2 = new CassandraConfigurationOverlay(overlay2Yaml, null);
        provider.storeOverlay(instance2, null, new ConfigurationOverlaySnapshot(Instant.now(), overlay2));

        ConfigurationManager manager = new ConfigurationManager(provider, configStore, BASE_TEMPLATE);
        manager.initialize(List.of(instance1, instance2));

        // Verify instance 1
        JsonObject config1 = ConfigUtils.loadYaml(configStore.resolve("1").resolve("cassandra.yaml"));
        assertThat(config1.getInteger("concurrent_reads")).isEqualTo(64);

        // Verify instance 2
        JsonObject config2 = ConfigUtils.loadYaml(configStore.resolve("2").resolve("cassandra.yaml"));
        assertThat(config2.getInteger("concurrent_reads")).isEqualTo(128);
    }

    @Test
    void testProviderUnavailablePreservesExistingCache() throws IOException
    {
        InstanceMetadata instance = mockInstance(1);

        // Pre-populate the config store with an existing overlay_cache.json
        Path instanceDir = configStore.resolve("1");
        Files.createDirectories(instanceDir);
        String existingContent = "{\"lastModified\":\"2026-01-01T00:00:00Z\",\"overlay\":{\"cassandraYaml\":{\"concurrent_reads\":32},\"extraJvmOpts\":{}}}";
        Files.writeString(instanceDir.resolve("overlay_cache.json"), existingContent, StandardCharsets.UTF_8);

        ConfigurationProvider failingProvider = new ConfigurationProvider()
        {
            @Override
            public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata inst)
            {
                throw new UncheckedIOException(new IOException("provider unavailable"));
            }

            @Override
            public boolean storeOverlay(InstanceMetadata inst, String originalHash,
                                        ConfigurationOverlaySnapshot newSnapshot)
            {
                throw new UnsupportedOperationException();
            }
        };

        ConfigurationManager manager = new ConfigurationManager(failingProvider, configStore, BASE_TEMPLATE);
        manager.initialize(List.of(instance));

        // Existing overlay_cache.json should be preserved
        assertThat(instanceDir.resolve("overlay_cache.json")).isRegularFile();
        String content = Files.readString(instanceDir.resolve("overlay_cache.json"), StandardCharsets.UTF_8);
        assertThat(content).isEqualTo(existingContent);
    }

    private static InstanceMetadata mockInstance(int id)
    {
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.id()).thenReturn(id);
        return instance;
    }
}
