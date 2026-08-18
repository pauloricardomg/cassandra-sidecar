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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods for configuration operations: YAML loading, deep merge, YAML writing,
 * and JVM option safety validation.
 */
public final class ConfigUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);

    static final YAMLFactory YAML_FACTORY = new YAMLFactory()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);

    // Allows: -Dproperty.name, -Xmx, -Xss, -XX:+Flag, -XX:-Flag, -XX:Flag
    // Rejects: -javaagent, -agentpath, -agentlib, keys with = or shell metacharacters
    public static final Pattern JVM_OPT_KEY_PATTERN = Pattern.compile(
            "^-(D[a-zA-Z][a-zA-Z0-9._-]*|X[a-z][a-zA-Z0-9]*|XX:[+-]?[a-zA-Z][a-zA-Z0-9_]*)$");

    // JVM options that are rejected because they execute arbitrary commands or write to
    // arbitrary filesystem paths. The value pattern permits absolute paths (Cassandra system
    // properties legitimately need them), so path-bearing flags must be blocked by key instead.
    public static final Set<String> BLOCKED_JVM_OPTS = Set.of(
            // Execute arbitrary commands
            "-XX:OnOutOfMemoryError",
            "-XX:OnError",
            // Write to arbitrary filesystem paths
            "-XX:ErrorFile",
            "-XX:HeapDumpPath",
            "-XX:LogFile",
            "-XX:FlightRecorderOptions",
            "-XX:StartFlightRecording",
            "-Xloggc",
            "-Xlog",
            "-Xbootclasspath");

    // Allows: alphanumeric, dots, colons, slashes, @, +, commas, hyphens, braces, brackets, quotes (max 512 chars).
    // Quotes/braces/brackets permit JSON values. Whitespace is rejected because the Cassandra launcher
    // word-splits it; shell metacharacters (;|&$`), newlines and other control characters are also rejected.
    public static final Pattern JVM_OPT_VALUE_PATTERN = Pattern.compile("^[a-zA-Z0-9._:/@+,\"{}\\[\\]-]{0,512}$");

    // Matches /../ path traversal sequences (start, middle, or end of path)
    public static final Pattern PATH_TRAVERSAL_PATTERN = Pattern.compile("(?:^|/)\\.\\.(?:/|$)");

    private ConfigUtils()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Validates whether a JVM option key/value pair is safe to pass to a Cassandra process.
     *
     * @param key   the full JVM option key including prefix (e.g. {@code -Dproperty.name})
     * @param value the option value, may be {@code null} or empty for boolean flags
     * @return {@code true} if the option is safe, {@code false} otherwise
     */
    public static boolean isValidJvmOption(@NotNull String key, @Nullable String value)
    {
        if (!JVM_OPT_KEY_PATTERN.matcher(key).matches())
        {
            return false;
        }
        if (BLOCKED_JVM_OPTS.contains(key))
        {
            return false;
        }
        if (value != null && !value.isEmpty())
        {
            if (!JVM_OPT_VALUE_PATTERN.matcher(value).matches())
            {
                return false;
            }
            if (PATH_TRAVERSAL_PATTERN.matcher(value).find())
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Writes a {@link JsonObject} as YAML to the specified path. The write is atomic: content is
     * first written to a temporary file in the same directory, then renamed to the target path.
     *
     * @param yamlPath the target file path
     * @param content  the configuration to write
     */
    public static void writeYaml(@NotNull Path yamlPath, @NotNull JsonObject content)
    {
        Objects.requireNonNull(yamlPath, "yamlPath must not be null");
        Objects.requireNonNull(content, "content must not be null");
        Path parentDir = yamlPath.getParent();
        if (parentDir == null)
        {
            throw new IllegalArgumentException("yamlPath must have a parent directory: " + yamlPath);
        }
        try
        {
            Files.createDirectories(parentDir);
            Path tempFile = Files.createTempFile(parentDir, "cassandra-yaml-", ".tmp");
            try
            {
                ObjectMapper yamlMapper = new ObjectMapper(YAML_FACTORY);
                // Encode to JSON string first to ensure all nested JsonObject/JsonArray types
                // are serialized as plain Maps/Lists that Jackson can write cleanly as YAML
                String json = content.encode();
                Object cleanMap = DatabindCodec.mapper().readValue(json, Object.class);
                Files.writeString(tempFile,
                        "# Auto-generated by Cassandra Sidecar configuration management. Do not modify.\n");
                yamlMapper.writeValue(Files.newOutputStream(tempFile,
                        java.nio.file.StandardOpenOption.APPEND), cleanMap);
                Files.move(tempFile, yamlPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            }
            catch (IOException e)
            {
                Files.deleteIfExists(tempFile);
                throw e;
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Failed to write YAML to " + yamlPath, e);
        }
    }

    /**
     * Loads configuration from the given YAML path, returning the cached snapshot if the file
     * has not been modified since it was last read.
     *
     * @param yamlPath the path to the YAML file, or {@code null} for an empty snapshot
     * @param cached   a previously loaded snapshot to reuse if the file is unchanged, or {@code null}
     * @return the cached snapshot if still valid, or a freshly loaded snapshot
     */
    public static ConfigurationOverlaySnapshot loadConfiguration(@Nullable Path yamlPath,
                                                                  @Nullable ConfigurationOverlaySnapshot cached)
    {
        if (yamlPath == null)
        {
            return ConfigurationOverlaySnapshot.emptySnapshot();
        }
        try
        {
            Instant lastModifiedBefore = Files.getLastModifiedTime(yamlPath).toInstant();
            if (cached != null && cached.lastModified().equals(lastModifiedBefore))
            {
                return cached;
            }
            JsonObject yaml = loadYaml(yamlPath);
            Instant lastModifiedAfter = Files.getLastModifiedTime(yamlPath).toInstant();
            if (!lastModifiedBefore.equals(lastModifiedAfter))
            {
                throw new IllegalStateException("File was modified while reading: " + yamlPath);
            }
            CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, Collections.emptyMap());
            return new ConfigurationOverlaySnapshot(lastModifiedAfter, overlay);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Failed to read modification time of " + yamlPath, e);
        }
    }

    /**
     * Loads a YAML file into a Vert.x {@link JsonObject}.
     *
     * @param yamlPath path to the YAML file
     * @return the parsed configuration as a JsonObject
     */
    @SuppressWarnings("unchecked")
    public static JsonObject loadYaml(Path yamlPath)
    {
        try (JsonParser parser = YAML_FACTORY.createParser(yamlPath.toFile()))
        {
            Map<String, Object> map = DatabindCodec.mapper().readValue(parser, Map.class);
            return map != null ? new JsonObject(map) : new JsonObject();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Failed to load YAML from " + yamlPath, e);
        }
    }

    /**
     * Deep-merges the overlay onto the base configuration. For nested objects both base and overlay
     * contain, fields are merged recursively. For all other node types (scalars, arrays, nulls),
     * the overlay value replaces the base value. The base node is not modified.
     *
     * <p>Overlays may introduce keys not present in the base configuration. Such keys are
     * added to the result as-is (scalars, arrays) or merged recursively (nested objects).
     *
     * @param base    the base configuration tree
     * @param overlay the overlay tree whose values take precedence
     * @return a new tree with the merged result
     */
    public static JsonObject mergeConfigurations(@NotNull JsonObject base, @NotNull JsonObject overlay)
    {
        Objects.requireNonNull(base, "base must not be null");
        Objects.requireNonNull(overlay, "overlay must not be null");
        JsonObject result = base.copy();
        result.mergeIn(overlay, true);
        return result;
    }

    /**
     * Merges overlay JVM options onto base JVM options, with overlay entries overriding base entries
     * on key conflict. When an overlay entry conflicts with a base boolean option (e.g. base
     * {@code -XX:+UseG1GC} and overlay {@code -XX:-UseG1GC}), the base option is preserved and the
     * overlay entry is skipped, matching {@link ConfigurationOverlaySnapshot#overlay}. Insertion order
     * is preserved. Neither input map is modified.
     *
     * @param base    the base JVM options
     * @param overlay the overlay JVM options whose values take precedence
     * @return a new map with the merged result
     */
    @NotNull
    public static Map<String, String> mergeOpts(@NotNull Map<String, String> base,
                                                @NotNull Map<String, String> overlay)
    {
        Objects.requireNonNull(base, "base must not be null");
        Objects.requireNonNull(overlay, "overlay must not be null");
        Map<String, String> merged = new LinkedHashMap<>(base);
        for (Map.Entry<String, String> entry : overlay.entrySet())
        {
            if (CassandraConfigurationOverlay.hasConflictingBooleanOpt(merged, entry.getKey()))
            {
                LOGGER.warn("Conflicting boolean JVM option '{}' in overlay conflicts with base option '{}'. " +
                            "Preserving base option and skipping overlay entry.",
                            entry.getKey(), CassandraConfigurationOverlay.conflictingBooleanOpt(entry.getKey()));
                continue; // preserve base boolean option, skip conflicting overlay entry
            }
            merged.put(entry.getKey(), entry.getValue());
        }
        return merged;
    }
}
