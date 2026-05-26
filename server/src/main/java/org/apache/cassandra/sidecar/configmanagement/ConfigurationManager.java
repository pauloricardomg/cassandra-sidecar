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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

/**
 * Manages configuration for different Cassandra instances
 *
 * <p>During initialization, the manager loads the base template, fetches overlays from the
 * {@link ConfigurationProvider}, merges them, and materializes effective configuration
 * artifacts ({@code cassandra.yaml}, {@code extra-jvm.options}, {@code overlay_cache.json})
 * to per-instance directories under the configuration store.
 */
public class ConfigurationManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationManager.class);

    static final String CASSANDRA_YAML = "cassandra.yaml";
    static final String EXTRA_JVM_OPTIONS = "extra-jvm.options";
    static final String OVERLAY_CACHE_JSON = "overlay_cache.json";

    private final ConfigurationProvider provider;
    private final Path configurationStore;
    private final Path baseTemplatePath;

    /**
     * @param provider           the configuration provider for fetching overlays
     * @param configurationStore root directory of the configuration store
     * @param baseTemplatePath   path to the base cassandra.yaml template
     */
    public ConfigurationManager(ConfigurationProvider provider,
                                Path configurationStore,
                                Path baseTemplatePath)
    {
        this.provider = Objects.requireNonNull(provider, "provider must not be null");
        this.configurationStore = Objects.requireNonNull(configurationStore, "configurationStore must not be null");
        this.baseTemplatePath = Objects.requireNonNull(baseTemplatePath, "baseTemplatePath must not be null");
    }

    /**
     * Initializes the configuration store for all managed instances. For each instance,
     * the base template is loaded, the overlay is fetched from the provider, and the
     * effective configuration is materialized to disk.
     *
     * @param instances the managed Cassandra instances
     */
    public void initialize(List<InstanceMetadata> instances)
    {
        LOGGER.info("Initializing configuration store at {} for {} instance(s)",
                     configurationStore, instances.size());
        for (InstanceMetadata instance : instances)
        {
            initializeInstance(instance);
        }
    }

    void initializeInstance(InstanceMetadata instance)
    {
        Path instanceDir = configurationStore.resolve(String.valueOf(instance.id()));
        LOGGER.info("Initializing configuration for instance {} at {}", instance.id(), instanceDir);

        JsonObject baseTemplate = ConfigUtils.loadYaml(baseTemplatePath);

        ConfigurationOverlaySnapshot snapshot;
        try
        {
            snapshot = provider.getOverlay(instance);
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to fetch overlay from provider for instance {}, " +
                         "skipping materialization", instance.id(), e);
            return;
        }

        JsonObject cassandraYaml;
        if (snapshot != null && !snapshot.overlay().cassandraYaml().isEmpty())
        {
            cassandraYaml = ConfigUtils.mergeConfigurations(baseTemplate, snapshot.overlay().cassandraYaml());
        }
        else
        {
            cassandraYaml = baseTemplate;
        }

        try
        {
            materializeCassandraYaml(instanceDir, cassandraYaml);
            materializeExtraJvmOptions(instanceDir, snapshot);
            materializeOverlayCache(instanceDir, snapshot);
        }
        catch (IOException e)
        {
            LOGGER.error("Failed to materialize configuration for instance {}", instance.id(), e);
        }
    }

    private void materializeCassandraYaml(Path instanceDir, JsonObject effectiveConfig) throws IOException
    {
        ConfigUtils.writeYaml(instanceDir.resolve(CASSANDRA_YAML), effectiveConfig);
    }

    private void materializeExtraJvmOptions(Path instanceDir, ConfigurationOverlaySnapshot snapshot) throws IOException
    {
        Path extraJvmOptsPath = instanceDir.resolve(EXTRA_JVM_OPTIONS);
        List<String> lines;
        if (snapshot != null && !snapshot.overlay().extraJvmOpts().isEmpty())
        {
            lines = ConfigUtils.formatJvmOptions(snapshot.overlay().extraJvmOpts());
        }
        else
        {
            lines = Collections.emptyList();
        }
        ConfigUtils.atomicWrite(extraJvmOptsPath,
                                tempPath -> Files.write(tempPath, lines, StandardCharsets.UTF_8));
    }

    private void materializeOverlayCache(Path instanceDir, ConfigurationOverlaySnapshot snapshot) throws IOException
    {
        if (snapshot == null)
        {
            return;
        }
        Path overlayJsonPath = instanceDir.resolve(OVERLAY_CACHE_JSON);
        ConfigUtils.atomicWrite(overlayJsonPath,
                                tempPath -> Files.writeString(tempPath,
                                                              snapshot.toJson().encodePrettily(),
                                                              StandardCharsets.UTF_8));
    }
}
