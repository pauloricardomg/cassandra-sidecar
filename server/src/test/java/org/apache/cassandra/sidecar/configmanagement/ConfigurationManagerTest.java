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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ConfigurationManager}
 */
class ConfigurationManagerTest
{
    private static final Path BASE_TEMPLATE = Paths.get("src/test/resources/configmanagement/cassandra_latest.yaml");

    private InMemoryConfigurationProvider provider;

    @BeforeEach
    void setUp()
    {
        provider = new InMemoryConfigurationProvider();
    }

    @Test
    void testGetEffectiveConfigurationNoOverlay()
    {
        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);

        ConfigurationOverlaySnapshot result = manager.getEffectiveConfiguration(instance);

        assertThat(result.configuration().cassandraYaml().getString("cluster_name")).isEqualTo("Test Cluster");
        assertThat(result.configuration().cassandraYaml().getInteger("num_tokens")).isEqualTo(16);
        assertThat(result.configuration().extraJvmOpts()).isEmpty();
        assertThat(result.lastModified()).isNotNull();
        assertThat(result.hash()).startsWith("sha256:");
    }

    @Test
    void testGetEffectiveConfigurationWithOverlay()
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

        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        ConfigurationOverlaySnapshot result = manager.getEffectiveConfiguration(instance);

        // Overlay values take precedence
        assertThat(result.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(128);
        assertThat(result.configuration().cassandraYaml().getInteger("memtable_flush_writers")).isEqualTo(8);
        // Base template values preserved
        assertThat(result.configuration().cassandraYaml().getString("cluster_name")).isEqualTo("Test Cluster");
        assertThat(result.configuration().cassandraYaml().getInteger("num_tokens")).isEqualTo(16);
        // JVM opts carried through
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Xmx", "4g");
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Dcassandra.ring_delay_ms", "60000");
    }

    @Test
    void testGetEffectiveConfigurationProviderFailure()
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

        ConfigurationManager manager = new ConfigurationManager(failingProvider, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);

        assertThatThrownBy(() -> manager.getEffectiveConfiguration(instance))
                .isInstanceOf(ConfigurationManagerException.class)
                .hasMessageContaining("Failed to get effective configuration")
                .hasCauseInstanceOf(UncheckedIOException.class);
    }

    @Test
    void testGetEffectiveConfigurationNullBaseTemplateNoOverlay()
    {
        ConfigurationManager manager = new ConfigurationManager(provider, null);
        InstanceMetadata instance = mockInstance(1);

        ConfigurationOverlaySnapshot result = manager.getEffectiveConfiguration(instance);

        assertThat(result.lastModified()).isEqualTo(Instant.EPOCH);
        assertThat(result.configuration().cassandraYaml()).isEmpty();
        assertThat(result.configuration().extraJvmOpts()).isEmpty();
        assertThat(result.hash()).startsWith("sha256:");
    }

    @Test
    void testGetEffectiveConfigurationNullBaseTemplateWithOverlay()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject yamlOverlay = new JsonObject().put("concurrent_reads", 128);
        Map<String, String> jvmOpts = Collections.singletonMap("-Xmx", "4g");

        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yamlOverlay, jvmOpts);
        Instant overlayTime = Instant.parse("2026-06-01T00:00:00Z");
        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(overlayTime, overlay);
        provider.storeOverlay(instance, null, snapshot);

        ConfigurationManager manager = new ConfigurationManager(provider, null);
        ConfigurationOverlaySnapshot result = manager.getEffectiveConfiguration(instance);

        assertThat(result.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(128);
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Xmx", "4g");
        assertThat(result.lastModified()).isEqualTo(overlayTime);
    }

    @Test
    void testGetEffectiveConfigurationCachesBaseSnapshot()
    {
        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);

        ConfigurationOverlaySnapshot first = manager.getEffectiveConfiguration(instance);
        ConfigurationOverlaySnapshot second = manager.getEffectiveConfiguration(instance);

        assertThat(second).isSameAs(first);
    }

    @Test
    void testPatchNoExistingOverlay()
    {
        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);

        ConfigurationOverlaySnapshot baseEffective = manager.getEffectiveConfiguration(instance);
        String baseHash = baseEffective.hash();

        Map<String, Object> yamlUpdates = new LinkedHashMap<>();
        yamlUpdates.put("concurrent_reads", 64);
        Map<String, String> jvmUpdates = new LinkedHashMap<>();
        jvmUpdates.put("-Xmx", "8g");

        ConfigurationOverlaySnapshot result = manager.patchConfiguration(instance, baseHash, yamlUpdates, jvmUpdates);

        assertThat(result.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Xmx", "8g");
        // Base values preserved
        assertThat(result.configuration().cassandraYaml().getString("cluster_name")).isEqualTo("Test Cluster");
        assertThat(result.hash()).startsWith("sha256:");
        assertThat(result.hash()).isNotEqualTo(baseHash);
    }

    @Test
    void testPatchWithExistingOverlay()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject initialYaml = new JsonObject().put("concurrent_reads", 64);
        Map<String, String> initialJvm = new LinkedHashMap<>();
        initialJvm.put("-Xmx", "4g");
        CassandraConfigurationOverlay initial = new CassandraConfigurationOverlay(initialYaml, initialJvm);
        provider.storeOverlay(instance, null, new ConfigurationOverlaySnapshot(Instant.now(), initial));

        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        String effectiveHash = manager.getEffectiveConfiguration(instance).hash();

        Map<String, Object> yamlUpdates = new LinkedHashMap<>();
        yamlUpdates.put("memtable_flush_writers", 4);

        ConfigurationOverlaySnapshot result = manager.patchConfiguration(instance, effectiveHash, yamlUpdates, null);

        // New value applied
        assertThat(result.configuration().cassandraYaml().getInteger("memtable_flush_writers")).isEqualTo(4);
        // Previous overlay value preserved
        assertThat(result.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
        // Previous JVM opts preserved
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Xmx", "4g");
        // Base value preserved
        assertThat(result.configuration().cassandraYaml().getString("cluster_name")).isEqualTo("Test Cluster");
    }

    @Test
    void testPatchRemovesOverlayField()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject initialYaml = new JsonObject().put("concurrent_reads", 128);
        CassandraConfigurationOverlay initial = new CassandraConfigurationOverlay(initialYaml, null);
        provider.storeOverlay(instance, null, new ConfigurationOverlaySnapshot(Instant.now(), initial));

        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        String effectiveHash = manager.getEffectiveConfiguration(instance).hash();

        Map<String, Object> yamlUpdates = new LinkedHashMap<>();
        yamlUpdates.put("concurrent_reads", null);

        ConfigurationOverlaySnapshot result = manager.patchConfiguration(instance, effectiveHash, yamlUpdates, null);

        // Removed from overlay, falls back to base template value
        assertThat(result.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(32);
    }

    @Test
    void testPatchPreservesUnchangedFields()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject initialYaml = new JsonObject().put("concurrent_reads", 64);
        Map<String, String> initialJvm = new LinkedHashMap<>();
        initialJvm.put("-Xmx", "4g");
        initialJvm.put("-Dcassandra.ring_delay_ms", "30000");
        CassandraConfigurationOverlay initial = new CassandraConfigurationOverlay(initialYaml, initialJvm);
        provider.storeOverlay(instance, null, new ConfigurationOverlaySnapshot(Instant.now(), initial));

        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        String effectiveHash = manager.getEffectiveConfiguration(instance).hash();

        // Patch only JVM opts, not cassandraYaml
        Map<String, String> jvmUpdates = new LinkedHashMap<>();
        jvmUpdates.put("-Xms", "2g");

        ConfigurationOverlaySnapshot result = manager.patchConfiguration(instance, effectiveHash, null, jvmUpdates);

        // cassandraYaml overlay preserved
        assertThat(result.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
        // Existing JVM opts preserved, new one added
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Xmx", "4g");
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Dcassandra.ring_delay_ms", "30000");
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Xms", "2g");
    }

    @Test
    void testPatchConflictStaleHash()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject initialYaml = new JsonObject().put("concurrent_reads", 64);
        CassandraConfigurationOverlay initial = new CassandraConfigurationOverlay(initialYaml, null);
        provider.storeOverlay(instance, null, new ConfigurationOverlaySnapshot(Instant.now(), initial));

        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        String actualHash = manager.getEffectiveConfiguration(instance).hash();
        String staleHash = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

        Map<String, Object> yamlUpdates = new LinkedHashMap<>();
        yamlUpdates.put("concurrent_reads", 128);

        assertThatThrownBy(() -> manager.patchConfiguration(instance, staleHash, yamlUpdates, null))
                .isInstanceOf(ConfigurationConflictException.class)
                .satisfies(e -> {
                    ConfigurationConflictException conflict = (ConfigurationConflictException) e;
                    assertThat(conflict.expectedHash()).isEqualTo(staleHash);
                    assertThat(conflict.actualHash()).isEqualTo(actualHash);
                });

        // Overlay unchanged
        assertThat(provider.getOverlay(instance).configuration().cassandraYaml().getInteger("concurrent_reads"))
                .isEqualTo(64);
    }

    @Test
    void testPatchStoreOverlayReturnsFalse()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject initialYaml = new JsonObject().put("concurrent_reads", 64);
        CassandraConfigurationOverlay initial = new CassandraConfigurationOverlay(initialYaml, null);
        ConfigurationOverlaySnapshot initialSnapshot = new ConfigurationOverlaySnapshot(Instant.now(), initial);

        ConfigurationProvider rejectingProvider = new ConfigurationProvider()
        {
            private ConfigurationOverlaySnapshot stored = initialSnapshot;

            @Override
            public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata inst)
            {
                return stored;
            }

            @Override
            public boolean storeOverlay(InstanceMetadata inst, String originalHash,
                                        ConfigurationOverlaySnapshot newSnapshot)
            {
                return false;
            }
        };

        ConfigurationManager manager = new ConfigurationManager(rejectingProvider, BASE_TEMPLATE);
        String effectiveHash = manager.getEffectiveConfiguration(instance).hash();

        Map<String, Object> yamlUpdates = Collections.singletonMap("concurrent_reads", 128);

        assertThatThrownBy(() -> manager.patchConfiguration(instance, effectiveHash, yamlUpdates, null))
                .isInstanceOf(ConfigurationManagerException.class)
                .isNotInstanceOf(ConfigurationConflictException.class)
                .hasMessageContaining("Provider rejected the overlay store unexpectedly");
    }

    @Test
    void testPatchStoreOverlayReturnsFalseWithConflict()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject initialYaml = new JsonObject().put("concurrent_reads", 64);
        CassandraConfigurationOverlay initial = new CassandraConfigurationOverlay(initialYaml, null);
        ConfigurationOverlaySnapshot initialSnapshot = new ConfigurationOverlaySnapshot(Instant.now(), initial);

        // Overlay changes between storeOverlay rejection and re-read
        JsonObject changedYaml = new JsonObject().put("concurrent_reads", 256);
        CassandraConfigurationOverlay changed = new CassandraConfigurationOverlay(changedYaml, null);
        ConfigurationOverlaySnapshot changedSnapshot = new ConfigurationOverlaySnapshot(Instant.now(), changed);

        AtomicInteger getOverlayCallCount = new AtomicInteger(0);
        ConfigurationProvider conflictingProvider = new ConfigurationProvider()
        {
            @Override
            public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata inst)
            {
                // First call returns initial, second call (after store rejection) returns changed
                return getOverlayCallCount.incrementAndGet() <= 1 ? initialSnapshot : changedSnapshot;
            }

            @Override
            public boolean storeOverlay(InstanceMetadata inst, String originalHash,
                                        ConfigurationOverlaySnapshot newSnapshot)
            {
                return false;
            }
        };

        ConfigurationManager manager = new ConfigurationManager(conflictingProvider, BASE_TEMPLATE);
        String effectiveHash = manager.getEffectiveConfiguration(instance).hash();

        Map<String, Object> yamlUpdates = Collections.singletonMap("concurrent_reads", 128);

        assertThatThrownBy(() -> manager.patchConfiguration(instance, effectiveHash, yamlUpdates, null))
                .isInstanceOf(ConfigurationConflictException.class)
                .satisfies(e -> {
                    ConfigurationConflictException conflict = (ConfigurationConflictException) e;
                    assertThat(conflict.expectedHash()).isEqualTo(effectiveHash);
                    assertThat(conflict.actualHash()).isNotEqualTo(effectiveHash);
                });
    }

    @Test
    void testPatchStoreOverlayThrows()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject initialYaml = new JsonObject().put("concurrent_reads", 64);
        CassandraConfigurationOverlay initial = new CassandraConfigurationOverlay(initialYaml, null);
        ConfigurationOverlaySnapshot initialSnapshot = new ConfigurationOverlaySnapshot(Instant.now(), initial);

        ConfigurationProvider failingStoreProvider = new ConfigurationProvider()
        {
            @Override
            public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata inst)
            {
                return initialSnapshot;
            }

            @Override
            public boolean storeOverlay(InstanceMetadata inst, String originalHash,
                                        ConfigurationOverlaySnapshot newSnapshot)
            {
                throw new UncheckedIOException(new IOException("storage unavailable"));
            }
        };

        ConfigurationManager manager = new ConfigurationManager(failingStoreProvider, BASE_TEMPLATE);
        String effectiveHash = manager.getEffectiveConfiguration(instance).hash();

        Map<String, Object> yamlUpdates = Collections.singletonMap("concurrent_reads", 128);

        assertThatThrownBy(() -> manager.patchConfiguration(instance, effectiveHash, yamlUpdates, null))
                .isInstanceOf(ConfigurationManagerException.class)
                .hasMessageContaining("Failed to patch configuration")
                .hasCauseInstanceOf(UncheckedIOException.class);
    }

    @Test
    void testPatchStoreRejectedReReadThrows()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject initialYaml = new JsonObject().put("concurrent_reads", 64);
        CassandraConfigurationOverlay initial = new CassandraConfigurationOverlay(initialYaml, null);
        ConfigurationOverlaySnapshot initialSnapshot = new ConfigurationOverlaySnapshot(Instant.now(), initial);

        AtomicInteger getOverlayCallCount = new AtomicInteger(0);
        ConfigurationProvider failOnReReadProvider = new ConfigurationProvider()
        {
            @Override
            public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata inst)
            {
                if (getOverlayCallCount.incrementAndGet() <= 1)
                {
                    return initialSnapshot;
                }
                throw new UncheckedIOException(new IOException("provider unavailable on re-read"));
            }

            @Override
            public boolean storeOverlay(InstanceMetadata inst, String originalHash,
                                        ConfigurationOverlaySnapshot newSnapshot)
            {
                return false;
            }
        };

        ConfigurationManager manager = new ConfigurationManager(failOnReReadProvider, BASE_TEMPLATE);
        String effectiveHash = manager.getEffectiveConfiguration(instance).hash();

        Map<String, Object> yamlUpdates = Collections.singletonMap("concurrent_reads", 128);

        assertThatThrownBy(() -> manager.patchConfiguration(instance, effectiveHash, yamlUpdates, null))
                .isInstanceOf(ConfigurationManagerException.class)
                .isNotInstanceOf(ConfigurationConflictException.class)
                .hasMessageContaining("Failed to patch configuration")
                .hasCauseInstanceOf(UncheckedIOException.class);
    }

    @Test
    void testPatchProviderFailure()
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
                                        @NotNull ConfigurationOverlaySnapshot newSnapshot)
            {
                throw new UnsupportedOperationException();
            }
        };

        ConfigurationManager manager = new ConfigurationManager(failingProvider, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);

        assertThatThrownBy(() -> manager.patchConfiguration(instance, "sha256:abc", null, null))
                .isInstanceOf(ConfigurationManagerException.class)
                .isNotInstanceOf(ConfigurationConflictException.class)
                .hasMessageContaining("Failed to patch configuration")
                .hasCauseInstanceOf(UncheckedIOException.class);
    }

    @Test
    void testPatchConcurrentSameInstance() throws Exception
    {
        InstanceMetadata instance = mockInstance(1);
        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        String baseHash = manager.getEffectiveConfiguration(instance).hash();

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger conflictCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++)
        {
            int value = i;
            futures.add(executor.submit(() -> {
                try
                {
                    startLatch.await();
                    Map<String, Object> yamlUpdates = Collections.singletonMap("concurrent_reads", value);
                    manager.patchConfiguration(instance, baseHash, yamlUpdates, null);
                    successCount.incrementAndGet();
                }
                catch (ConfigurationConflictException e)
                {
                    conflictCount.incrementAndGet();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        startLatch.countDown();
        for (Future<?> future : futures)
        {
            future.get();
        }
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(1);
        assertThat(conflictCount.get()).isEqualTo(threadCount - 1);
    }

    @Test
    void testPatchInvalidJvmOptKey()
    {
        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);
        String baseHash = manager.getEffectiveConfiguration(instance).hash();

        Map<String, String> jvmUpdates = new LinkedHashMap<>();
        jvmUpdates.put("invalidKey", "value");

        assertThatThrownBy(() -> manager.patchConfiguration(instance, baseHash, null, jvmUpdates))
                .isInstanceOf(ConfigurationManagerException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Invalid JVM option key 'invalidKey': must be a system property (-D),"
                                    + " advanced option (-XX:), or non-standard option (-X)");
    }

    private static InstanceMetadata mockInstance(int id)
    {
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.id()).thenReturn(id);
        return instance;
    }
}
