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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;

/**
 * Utility methods for configuration store operations: YAML loading, deep merge,
 * JVM option formatting, and atomic file writes.
 */
public final class ConfigUtils
{
    static final YAMLFactory YAML_FACTORY = new YAMLFactory()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);

    private ConfigUtils()
    {
        throw new UnsupportedOperationException();
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
     * Atomically writes a {@link JsonObject} as a YAML file.
     *
     * @param target the final file path
     * @param config the configuration to write as YAML
     */
    public static void writeYaml(Path target, JsonObject config) throws IOException
    {
        atomicWrite(target, tempPath -> {
            try (JsonGenerator gen = YAML_FACTORY.createGenerator(tempPath.toFile(), JsonEncoding.UTF8))
            {
                gen.setCodec(DatabindCodec.mapper());
                DatabindCodec.mapper().writer(SerializationFeature.INDENT_OUTPUT)
                             .writeValue(gen, config.getMap());
            }
        });
    }

    /**
     * Deep-merges the overlay onto the base configuration. For nested objects both base and overlay
     * contain, fields are merged recursively. For all other node types (scalars, arrays, nulls),
     * the overlay value replaces the base value. The base node is not modified.
     *
     * @param base    the base configuration tree
     * @param overlay the overlay tree whose values take precedence
     * @return a new tree with the merged result
     */
    public static JsonObject mergeConfigurations(JsonObject base, JsonObject overlay)
    {
        if (base == null)
        {
            return overlay.copy();
        }
        if (overlay == null || overlay.isEmpty())
        {
            return base.copy();
        }

        JsonObject result = base.copy();
        JsonObject overlayCopy = overlay.copy();
        for (Map.Entry<String, Object> field : overlayCopy)
        {
            String fieldName = field.getKey();
            Object overlayValue = field.getValue();
            Object baseValue = result.getValue(fieldName);

            if (isJsonObject(baseValue) && isJsonObject(overlayValue))
            {
                JsonObject merged = mergeConfigurations(
                        asJsonObject(baseValue),
                        asJsonObject(overlayValue));
                result.put(fieldName, merged);
            }
            else
            {
                result.put(fieldName, overlayValue);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static boolean isJsonObject(Object value)
    {
        return value instanceof JsonObject || value instanceof Map;
    }

    @SuppressWarnings("unchecked")
    private static JsonObject asJsonObject(Object value)
    {
        if (value instanceof JsonObject)
        {
            return (JsonObject) value;
        }
        return new JsonObject((Map<String, Object>) value);
    }

    /**
     * Formats a map of JVM options into a list of command-line argument strings.
     *
     * <p>Serialization rules per JVM flag type:
     * <ul>
     *   <li>{@code -X} (non-standard, not {@code -XX}): direct concatenation (e.g., {@code -Xmx4g})</li>
     *   <li>{@code -XX:} (advanced value): equals sign (e.g., {@code -XX:ParallelGCThreads=8})</li>
     *   <li>{@code -XX:+/-} (advanced boolean): standalone, no value (e.g., {@code -XX:+PrintGCDetails})</li>
     *   <li>{@code -D} (system property): equals sign (e.g., {@code -Dmy.prop=value})</li>
     *   <li>null/empty value: key alone</li>
     * </ul>
     *
     * @param opts map of JVM option key to value
     * @return list of formatted option strings, one per entry
     */
    public static List<String> formatJvmOptions(Map<String, String> opts)
    {
        List<String> lines = new ArrayList<>(opts.size());
        for (Map.Entry<String, String> entry : opts.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value == null || value.isEmpty())
            {
                lines.add(key);
            }
            else if (key.startsWith("-X") && !key.startsWith("-XX"))
            {
                lines.add(key + value);
            }
            else
            {
                lines.add(key + "=" + value);
            }
        }
        return lines;
    }

    /**
     * Atomically writes content to a target path. Writes to a temporary file first,
     * then atomically moves it to the target to prevent corruption from crashes.
     *
     * @param target the final file path
     * @param writer a consumer that writes content to the provided temporary path
     */
    public static void atomicWrite(Path target, IOConsumer writer) throws IOException
    {
        Files.createDirectories(target.getParent());
        Path tempFile = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
        try
        {
            writer.accept(tempFile);
            Files.move(tempFile, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            try
            {
                Files.deleteIfExists(tempFile);
            }
            catch (IOException suppressed)
            {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /**
     * A consumer that accepts a path and may throw {@link IOException}.
     */
    @FunctionalInterface
    public interface IOConsumer
    {
        void accept(Path path) throws IOException;
    }
}
