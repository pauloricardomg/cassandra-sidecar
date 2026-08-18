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
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * File-based implementation of {@link ConfigurationProvider} that persists configuration
 * overlays as JSON files within a configuration store directory.
 *
 * <p>Each instance's overlay is stored at {@code {configDir}/{instanceId}/overlay.json}.
 * Writes are atomic (write to temp file, then rename) to prevent corruption from crashes.
 *
 * <p>Reads always go to disk so the provider never serves stale data, including when
 * {@code overlay.json} is modified out-of-band. Writes acquire a per-instance lock to serialize the
 * read-modify-write against concurrent writers.
 */
public class FileBasedConfigurationProvider implements ConfigurationProvider
{
    static final String DEFAULT_OVERLAY_FILE_NAME = "overlay.json";

    private final Path configDir;
    private final String configFileName;
    private final ConcurrentHashMap<Integer, Object> locks = new ConcurrentHashMap<>();

    public FileBasedConfigurationProvider(Path configDir)
    {
        this(configDir, DEFAULT_OVERLAY_FILE_NAME);
    }

    public FileBasedConfigurationProvider(Path configDir, String configFileName)
    {
        this.configDir = Objects.requireNonNull(configDir, "configDir must not be null");
        this.configFileName = Objects.requireNonNull(configFileName, "configFileName must not be null");
    }

    @Override
    @Nullable
    public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata instance)
    {
        return readFromDisk(instance);
    }

    @Override
    public boolean storeOverlay(InstanceMetadata instance,
                                @Nullable String originalHash,
                                @NotNull ConfigurationOverlaySnapshot newSnapshot)
    {
        Objects.requireNonNull(newSnapshot, "newSnapshot must not be null");
        Object lock = locks.computeIfAbsent(instance.id(), k -> new Object());
        synchronized (lock)
        {
            ConfigurationOverlaySnapshot current = readFromDisk(instance);

            if (current == null && originalHash != null)
            {
                return false;
            }

            if (current != null && (originalHash == null || !current.hash().equals(originalHash)))
            {
                return false;
            }

            writeToDisk(instance, newSnapshot);
            return true;
        }
    }

    @Nullable
    private ConfigurationOverlaySnapshot readFromDisk(InstanceMetadata instance)
    {
        Path configFile = resolveInstanceDir(instance).resolve(configFileName);
        if (!Files.exists(configFile))
        {
            return null;
        }
        try
        {
            String content = Files.readString(configFile, StandardCharsets.UTF_8);
            return ConfigurationOverlaySnapshot.fromJson(new JsonObject(content));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Failed to read configuration overlay for instance " + instance.id(), e);
        }
    }

    private void writeToDisk(InstanceMetadata instance, ConfigurationOverlaySnapshot snapshot)
    {
        Path instanceDir = resolveInstanceDir(instance);
        Path configFile = instanceDir.resolve(configFileName);
        Path tempFile = null;
        try
        {
            Files.createDirectories(instanceDir);
            tempFile = Files.createTempFile(instanceDir, "config", ".tmp");
            Files.writeString(tempFile, snapshot.toJson().encodePrettily(), StandardCharsets.UTF_8);
            Files.move(tempFile, configFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            if (tempFile != null)
            {
                try
                {
                    Files.deleteIfExists(tempFile);
                }
                catch (IOException suppressed)
                {
                    e.addSuppressed(suppressed);
                }
            }
            throw new UncheckedIOException("Failed to store configuration overlay for instance " + instance.id(), e);
        }
    }

    private Path resolveInstanceDir(InstanceMetadata instance)
    {
        return configDir.resolve(String.valueOf(instance.id()));
    }
}
