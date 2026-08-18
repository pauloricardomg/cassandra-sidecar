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

package org.apache.cassandra.sidecar.routes;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.replacePlaceholdersInFileWithPattern;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the configuration management GET and PATCH endpoints.
 * Tests the end-to-end workflow: GET -> PATCH -> GET with a real
 * {@link org.apache.cassandra.sidecar.configmanagement.FileBasedConfigurationProvider}.
 */
@ExtendWith(VertxExtension.class)
class ConfigurationManagementIntegrationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationManagementIntegrationTest.class);
    private static final String CONFIGURATION_ROUTE = "/api/v1/cassandra/configuration";
    private static final String JSON_PATCH_CONTENT_TYPE = "application/json-patch+json";
    private static final int TIMEOUT_SECONDS = 30;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    private static Path tmpDir;

    private static Path configStorePath;
    private static Server server;
    private static Vertx vertx;
    private static WebClient client;
    private static String sidecarDeploymentId;

    @BeforeAll
    static void setup() throws Exception
    {
        configStorePath = Files.createDirectories(tmpDir.resolve("config-store"));
        Path storageDir = Files.createDirectories(tmpDir.resolve("storage"));

        String cassandraYamlPath = resolveTestCassandraYaml();
        Path sidecarYaml = createSidecarYaml(configStorePath, storageDir, cassandraYamlPath);

        LOGGER.info("Starting sidecar with config at: {}", sidecarYaml);
        Injector injector = Guice.createInjector(SidecarModules.all(sidecarYaml));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        client = WebClient.create(vertx);
        sidecarDeploymentId = server.start().toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
        LOGGER.info("Sidecar started on port {}", server.actualPort());
    }

    @AfterAll
    static void tearDown() throws ExecutionException, InterruptedException, TimeoutException
    {
        if (server != null && sidecarDeploymentId != null)
        {
            server.stop(sidecarDeploymentId).toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
        }
    }

    @BeforeEach
    void clearOverlays() throws IOException
    {
        if (Files.exists(configStorePath))
        {
            Files.walkFileTree(configStorePath, new SimpleFileVisitor<>()
            {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
                {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
                {
                    if (!dir.equals(configStorePath))
                    {
                        Files.delete(dir);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    @Test
    void testGetPatchGetWorkflow()
    {
        // 1. GET initial configuration
        HttpResponse<Buffer> getResponse1 = getConfiguration();
        assertThat(getResponse1.statusCode()).isEqualTo(200);
        JsonObject body1 = getResponse1.bodyAsJsonObject();
        String initialEtag = getResponse1.getHeader("ETag");
        assertThat(initialEtag).isEqualTo(body1.getString("hash"));

        JsonObject cassandraYaml1 = body1.getJsonObject("configuration").getJsonObject("cassandraYaml");
        int initialConcurrentReads = cassandraYaml1.getInteger("concurrent_reads");
        String clusterName = cassandraYaml1.getString("cluster_name");

        // 2. PATCH concurrent_reads
        int newValue = initialConcurrentReads * 2;
        JsonArray patchOps = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", newValue));

        HttpResponse<Buffer> patchResponse = patchConfiguration(initialEtag, patchOps);
        assertThat(patchResponse.statusCode()).isEqualTo(200);
        String patchEtag = patchResponse.getHeader("ETag");
        assertThat(patchEtag).isNotEqualTo(initialEtag);
        JsonObject patchBody = patchResponse.bodyAsJsonObject();
        assertThat(patchBody.getString("hash")).isEqualTo(patchEtag);
        assertThat(patchBody.getJsonObject("configuration").getJsonObject("cassandraYaml")
                            .getInteger("concurrent_reads")).isEqualTo(newValue);

        // 3. GET again to verify persistence
        HttpResponse<Buffer> getResponse2 = getConfiguration();
        assertThat(getResponse2.statusCode()).isEqualTo(200);
        assertThat(getResponse2.getHeader("ETag")).isEqualTo(patchEtag);
        JsonObject cassandraYaml2 = getResponse2.bodyAsJsonObject().getJsonObject("configuration")
                                                 .getJsonObject("cassandraYaml");
        assertThat(cassandraYaml2.getInteger("concurrent_reads")).isEqualTo(newValue);
        assertThat(cassandraYaml2.getString("cluster_name")).isEqualTo(clusterName);
    }

    @Test
    void testPatchWithStaleHashReturns409()
    {
        // 1. GET initial config
        HttpResponse<Buffer> getResponse = getConfiguration();
        String initialEtag = getResponse.getHeader("ETag");

        // 2. PATCH with correct ETag
        JsonArray patchOps = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 99));

        HttpResponse<Buffer> patchResponse = patchConfiguration(initialEtag, patchOps);
        assertThat(patchResponse.statusCode()).isEqualTo(200);

        // 3. PATCH again with the now-stale ETag
        HttpResponse<Buffer> stalePatchResponse = patchConfiguration(initialEtag, patchOps);
        assertThat(stalePatchResponse.statusCode()).isEqualTo(409);
    }

    @Test
    void testMultipleOperationsInOnePatch()
    {
        HttpResponse<Buffer> getResponse = getConfiguration();
        String etag = getResponse.getHeader("ETag");

        JsonArray patchOps = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 64))
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_writes")
                                     .put("value", 128));

        HttpResponse<Buffer> patchResponse = patchConfiguration(etag, patchOps);
        assertThat(patchResponse.statusCode()).isEqualTo(200);

        HttpResponse<Buffer> getResponse2 = getConfiguration();
        JsonObject yaml = getResponse2.bodyAsJsonObject().getJsonObject("configuration")
                                       .getJsonObject("cassandraYaml");
        assertThat(yaml.getInteger("concurrent_reads")).isEqualTo(64);
        assertThat(yaml.getInteger("concurrent_writes")).isEqualTo(128);
    }

    @Test
    void testPatchAddThenRemoveRestoresBaseValue()
    {
        // 1. Capture initial base value
        HttpResponse<Buffer> getResponse1 = getConfiguration();
        int initialConcurrentReads = getResponse1.bodyAsJsonObject().getJsonObject("configuration")
                                                  .getJsonObject("cassandraYaml")
                                                  .getInteger("concurrent_reads");
        String etag1 = getResponse1.getHeader("ETag");

        // 2. Replace concurrent_reads
        JsonArray replaceOps = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 99));
        HttpResponse<Buffer> patchResponse1 = patchConfiguration(etag1, replaceOps);
        assertThat(patchResponse1.statusCode()).isEqualTo(200);
        String etag2 = patchResponse1.getHeader("ETag");

        // 3. Verify modified value
        HttpResponse<Buffer> getResponse2 = getConfiguration();
        assertThat(getResponse2.bodyAsJsonObject().getJsonObject("configuration")
                                .getJsonObject("cassandraYaml")
                                .getInteger("concurrent_reads")).isEqualTo(99);

        // 4. Remove the overlay entry
        JsonArray removeOps = new JsonArray()
                .add(new JsonObject().put("op", "remove")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads"));
        HttpResponse<Buffer> patchResponse2 = patchConfiguration(etag2, removeOps);
        assertThat(patchResponse2.statusCode()).isEqualTo(200);

        // 5. Verify base template value is restored
        HttpResponse<Buffer> getResponse3 = getConfiguration();
        assertThat(getResponse3.bodyAsJsonObject().getJsonObject("configuration")
                                .getJsonObject("cassandraYaml")
                                .getInteger("concurrent_reads")).isEqualTo(initialConcurrentReads);
    }

    @Test
    void testPatchNestedConfiguration()
    {
        HttpResponse<Buffer> getResponse = getConfiguration();
        String etag = getResponse.getHeader("ETag");
        JsonObject serverEncryption = getResponse.bodyAsJsonObject().getJsonObject("configuration")
                                                  .getJsonObject("cassandraYaml")
                                                  .getJsonObject("server_encryption_options");
        assertThat(serverEncryption.getString("internode_encryption")).isEqualTo("none");

        JsonArray patchOps = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/server_encryption_options/internode_encryption")
                                     .put("value", "all"));

        HttpResponse<Buffer> patchResponse = patchConfiguration(etag, patchOps);
        assertThat(patchResponse.statusCode()).isEqualTo(200);

        HttpResponse<Buffer> getResponse2 = getConfiguration();
        JsonObject updatedEncryption = getResponse2.bodyAsJsonObject().getJsonObject("configuration")
                                                    .getJsonObject("cassandraYaml")
                                                    .getJsonObject("server_encryption_options");
        assertThat(updatedEncryption.getString("internode_encryption")).isEqualTo("all");
        assertThat(updatedEncryption.getString("keystore")).isEqualTo("conf/.keystore");
    }

    @Test
    void testTestOperationGuardsReplace()
    {
        HttpResponse<Buffer> getResponse = getConfiguration();
        String etag = getResponse.getHeader("ETag");
        int initialValue = getResponse.bodyAsJsonObject().getJsonObject("configuration")
                                       .getJsonObject("cassandraYaml")
                                       .getInteger("concurrent_reads");

        JsonArray patchOps = new JsonArray()
                .add(new JsonObject().put("op", "test")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", initialValue))
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 128));

        HttpResponse<Buffer> patchResponse = patchConfiguration(etag, patchOps);
        assertThat(patchResponse.statusCode()).isEqualTo(200);

        HttpResponse<Buffer> getResponse2 = getConfiguration();
        assertThat(getResponse2.bodyAsJsonObject().getJsonObject("configuration")
                                .getJsonObject("cassandraYaml")
                                .getInteger("concurrent_reads")).isEqualTo(128);
    }

    private HttpResponse<Buffer> getConfiguration()
    {
        return getBlocking(client.get(server.actualPort(), "localhost", CONFIGURATION_ROUTE).send());
    }

    private HttpResponse<Buffer> patchConfiguration(String ifMatch, JsonArray operations)
    {
        return getBlocking(client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
                                 .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
                                 .putHeader("If-Match", ifMatch)
                                 .sendBuffer(operations.toBuffer()));
    }

    private static String resolveTestCassandraYaml()
    {
        Path projectRoot = Path.of(System.getProperty("user.dir")).getParent();
        return projectRoot.resolve("server/src/test/resources/configmanagement/cassandra_latest.yaml")
                          .toAbsolutePath()
                          .toString();
    }

    private static Path createSidecarYaml(Path configStore, Path storageDir, String cassandraYamlPath)
    throws IOException, URISyntaxException
    {
        Path sidecarConfDir = Files.createDirectories(tmpDir.resolve("etc/sidecar"));
        URL templateUrl = ConfigurationManagementIntegrationTest.class
                .getResource("/config/sidecar_configuration_management.yaml.template");
        Path templatePath = Path.of(Objects.requireNonNull(templateUrl).toURI());
        Path sidecarYaml = sidecarConfDir.resolve("sidecar.yaml");
        replacePlaceholdersInFileWithPattern(templatePath,
                                            Map.of("configurationStore", configStore.toString(),
                                                   "storageDir", storageDir.toString(),
                                                   "cassandraYamlTemplate", cassandraYamlPath),
                                            sidecarYaml);
        return sidecarYaml;
    }
}
