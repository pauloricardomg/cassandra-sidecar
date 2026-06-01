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

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Manages configuration of Cassandra instances via Sidecar
 */
public class ConfigurationManager
{
    private final ConfigurationProvider provider;
    @Nullable
    private final Path baseTemplatePath;
    private final ConcurrentHashMap<Integer, Object> instanceLocks = new ConcurrentHashMap<>();

    @Nullable
    private volatile ConfigurationOverlaySnapshot cachedBaseSnapshot;

    /**
     * @param provider         the configuration provider for fetching overlays
     * @param baseTemplatePath path to the base cassandra.yaml template, or {@code null} for an empty base
     */
    public ConfigurationManager(ConfigurationProvider provider, @Nullable Path baseTemplatePath)
    {
        this.provider = Objects.requireNonNull(provider, "provider must not be null");
        this.baseTemplatePath = baseTemplatePath;
    }

    /**
     * Computes the effective configuration for the given instance by merging the base template
     * with the overlay from the {@link ConfigurationProvider}.
     *
     * @param instance the Cassandra instance metadata
     * @return a snapshot of the effective configuration with its SHA-256 hash and last modified timestamp
     * @throws ConfigurationManagerException if the provider fails to retrieve the overlay
     */
    @NotNull
    public ConfigurationOverlaySnapshot getEffectiveConfiguration(InstanceMetadata instance)
    {
        ConfigurationOverlaySnapshot baseSnapshot = getBaseSnapshot();

        ConfigurationOverlaySnapshot providerSnapshot;
        try
        {
            providerSnapshot = provider.getOverlay(instance);
        }
        catch (Exception e)
        {
            throw new ConfigurationManagerException(
                    "Failed to retrieve configuration overlay from provider", e);
        }

        if (providerSnapshot != null)
        {
            return baseSnapshot.overlay(providerSnapshot, instance.id());
        }
        return baseSnapshot;
    }

    private ConfigurationOverlaySnapshot getBaseSnapshot()
    {
        ConfigurationOverlaySnapshot snapshot = ConfigUtils.loadConfiguration(baseTemplatePath, cachedBaseSnapshot);
        cachedBaseSnapshot = snapshot;
        return snapshot;
    }

    /**
     * Patches the effective configuration for the given instance. Validates the caller's expected hash
     * against the current effective configuration, applies the updates to the overlay, persists via the
     * {@link ConfigurationProvider}, and returns the new effective configuration.
     *
     * @param instance              the Cassandra instance metadata
     * @param expectedHash          the hash of the effective configuration as last seen by the caller
     * @param cassandraYamlUpdates  field-level changes to cassandra.yaml; null values remove fields;
     *                              pass {@code null} for no yaml changes
     * @param extraJvmOptsUpdates   JVM option changes; null values remove options;
     *                              pass {@code null} for no changes
     * @return a snapshot of the new effective configuration with its SHA-256 hash and last modified timestamp
     * @throws ConfigurationConflictException if the expectedHash does not match the current effective hash
     * @throws ConfigurationManagerException  if the provider fails during the operation
     */
    @NotNull
    public ConfigurationOverlaySnapshot patchConfiguration(@NotNull InstanceMetadata instance,
                                                           @NotNull String expectedHash,
                                                           @Nullable Map<String, Object> cassandraYamlUpdates,
                                                           @Nullable Map<String, String> extraJvmOptsUpdates)
    {
        Objects.requireNonNull(instance, "instance must not be null");
        Objects.requireNonNull(expectedHash, "expectedHash must not be null");

        synchronized (instanceLocks.computeIfAbsent(instance.id(), k -> new Object()))
        {
            ConfigurationOverlaySnapshot baseSnapshot = getBaseSnapshot();

            ConfigurationOverlaySnapshot currentOverlay;
            try
            {
                currentOverlay = provider.getOverlay(instance);
            }
            catch (Exception e)
            {
                throw new ConfigurationManagerException(
                        "Failed to retrieve configuration overlay from provider", e);
            }

            ConfigurationOverlaySnapshot effectiveConfig = currentOverlay != null
                                                           ? baseSnapshot.overlay(currentOverlay, instance.id())
                                                           : baseSnapshot;

            if (!expectedHash.equals(effectiveConfig.hash()))
            {
                throw new ConfigurationConflictException(expectedHash, effectiveConfig.hash());
            }

            CassandraConfigurationOverlay currentOverlayConfig = currentOverlay != null
                                                                 ? currentOverlay.configuration()
                                                                 : new CassandraConfigurationOverlay(null, null);
            CassandraConfigurationOverlay updatedOverlay = currentOverlayConfig.updated(cassandraYamlUpdates,
                                                                                        extraJvmOptsUpdates);
            ConfigurationOverlaySnapshot newOverlaySnapshot = new ConfigurationOverlaySnapshot(Instant.now(),
                                                                                               updatedOverlay);

            String currentOverlayHash = currentOverlay != null ? currentOverlay.hash() : null;
            boolean stored;
            try
            {
                stored = provider.storeOverlay(instance, currentOverlayHash, newOverlaySnapshot);
            }
            catch (Exception e)
            {
                throw new ConfigurationManagerException("Failed to store configuration overlay", e);
            }

            if (!stored)
            {
                ConfigurationOverlaySnapshot updatedCurrent;
                try
                {
                    updatedCurrent = provider.getOverlay(instance);
                }
                catch (Exception e)
                {
                    throw new ConfigurationManagerException(
                            "Failed to retrieve configuration overlay after store conflict", e);
                }
                ConfigurationOverlaySnapshot newEffective = updatedCurrent != null
                                                            ? baseSnapshot.overlay(updatedCurrent, instance.id())
                                                            : baseSnapshot;
                if (!expectedHash.equals(newEffective.hash()))
                {
                    throw new ConfigurationConflictException(expectedHash, newEffective.hash());
                }
                throw new ConfigurationManagerException(
                        "Provider rejected the overlay store unexpectedly", null);
            }

            return baseSnapshot.overlay(newOverlaySnapshot, instance.id());
        }
    }
}
