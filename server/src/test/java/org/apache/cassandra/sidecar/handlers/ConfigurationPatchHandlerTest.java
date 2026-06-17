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

package org.apache.cassandra.sidecar.handlers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.configmanagement.CassandraConfigurationOverlay;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationConflictException;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationManager;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationManagerException;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationOverlaySnapshot;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchException;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationProviderUnavailableException;
import org.apache.cassandra.sidecar.modules.ConfigurationManagementModule;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link ConfigurationPatchHandler}
 */
@ExtendWith(VertxExtension.class)
public class ConfigurationPatchHandlerTest
{
    private static final String CONFIGURATION_ROUTE = "/api/v1/cassandra/configuration";
    private static final String JSON_PATCH_CONTENT_TYPE = "application/json-patch+json";

    Vertx vertx;
    Server server;
    ConfigurationManager mockConfigurationManager = mock(ConfigurationManager.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Module testOverride = Modules.override(new TestModule()).with(new ConfigurationPatchHandlerTestModule());
        List<Module> modules = new ArrayList<>(SidecarModules.all());
        modules.add(new ConfigurationManagementModule());
        Injector injector = Guice.createInjector(Modules.override(modules).with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start().onSuccess(s -> context.completeNow()).onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        getBlocking(TestResourceReaper.create().with(server).close(), 60, TimeUnit.SECONDS, "Closing server");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSuccessfulPatch(VertxTestContext context)
    {
        Instant lastModified = Instant.parse("2026-06-01T12:00:00Z");
        JsonObject yamlConfig = new JsonObject().put("concurrent_reads", 64);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yamlConfig, Collections.emptyMap());
        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(lastModified, overlay);

        when(mockConfigurationManager.patchConfiguration(any(InstanceMetadata.class), anyString(), anyList()))
                .thenReturn(snapshot);

        JsonArray patchBody = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 64));

        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
              .putHeader("If-Match", "sha256:abc123")
              .sendBuffer(patchBody.toBuffer(), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(200);
                  JsonObject body = response.bodyAsJsonObject();
                  assertThat(body.getString("hash")).isEqualTo(snapshot.hash());
                  assertThat(body.getString("lastModified")).isEqualTo(lastModified.toString());
                  assertThat(response.getHeader("ETag")).isEqualTo(snapshot.hash());
                  assertThat(response.getHeader("ETag")).isEqualTo(body.getString("hash"));
                  context.completeNow();
              }));
    }

    @Test
    void testWrongContentType(VertxTestContext context)
    {
        JsonArray patchBody = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 64));

        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", "application/json")
              .putHeader("If-Match", "sha256:abc123")
              .sendBuffer(patchBody.toBuffer(), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(415);
                  context.completeNow();
              }));
    }

    @Test
    void testMissingIfMatchHeader(VertxTestContext context)
    {
        JsonArray patchBody = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 64));

        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
              .sendBuffer(patchBody.toBuffer(), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(428);
                  context.completeNow();
              }));
    }

    @Test
    void testMissingBody(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
              .putHeader("If-Match", "sha256:abc123")
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(400);
                  context.completeNow();
              }));
    }

    @Test
    void testEmptyOperationsArray(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
              .putHeader("If-Match", "sha256:abc123")
              .sendBuffer(new JsonArray().toBuffer(), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(400);
                  context.completeNow();
              }));
    }

    @Test
    void testMalformedJsonBody(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
              .putHeader("If-Match", "sha256:abc123")
              .sendBuffer(Buffer.buffer("not valid json"), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(400);
                  context.completeNow();
              }));
    }

    @Test
    void testUnsupportedOperation(VertxTestContext context)
    {
        JsonArray patchBody = new JsonArray()
                .add(new JsonObject().put("op", "move")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("from", "/configuration/cassandraYaml/concurrent_writes"));

        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
              .putHeader("If-Match", "sha256:abc123")
              .sendBuffer(patchBody.toBuffer(), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(422);
                  context.completeNow();
              }));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testConflictHashMismatch(VertxTestContext context)
    {
        when(mockConfigurationManager.patchConfiguration(any(InstanceMetadata.class), anyString(), anyList()))
                .thenThrow(new ConfigurationConflictException("sha256:expected", "sha256:actual"));

        JsonArray patchBody = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 64));

        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
              .putHeader("If-Match", "sha256:stale")
              .sendBuffer(patchBody.toBuffer(), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(409);
                  assertThat(response.getHeader("ETag")).isEqualTo("sha256:actual");
                  context.completeNow();
              }));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPatchValidationFailure(VertxTestContext context)
    {
        when(mockConfigurationManager.patchConfiguration(any(InstanceMetadata.class), anyString(), anyList()))
                .thenThrow(new ConfigurationPatchException("Duplicate path in patch: '/configuration/cassandraYaml/concurrent_reads'", null));

        JsonArray patchBody = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 64));

        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
              .putHeader("If-Match", "sha256:abc123")
              .sendBuffer(patchBody.toBuffer(), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(400);
                  context.completeNow();
              }));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testProviderUnavailable(VertxTestContext context)
    {
        when(mockConfigurationManager.patchConfiguration(any(InstanceMetadata.class), anyString(), anyList()))
                .thenThrow(new ConfigurationProviderUnavailableException("Provider is down", null));

        JsonArray patchBody = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 64));

        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
              .putHeader("If-Match", "sha256:abc123")
              .sendBuffer(patchBody.toBuffer(), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(503);
                  context.completeNow();
              }));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testInternalError(VertxTestContext context)
    {
        when(mockConfigurationManager.patchConfiguration(any(InstanceMetadata.class), anyString(), anyList()))
                .thenThrow(new ConfigurationManagerException("Internal error", null));

        JsonArray patchBody = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_reads")
                                     .put("value", 64));

        WebClient client = WebClient.create(vertx);
        client.patch(server.actualPort(), "localhost", CONFIGURATION_ROUTE)
              .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
              .putHeader("If-Match", "sha256:abc123")
              .sendBuffer(patchBody.toBuffer(), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(500);
                  context.completeNow();
              }));
    }

    class ConfigurationPatchHandlerTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public ConfigurationManager configurationManager()
        {
            return mockConfigurationManager;
        }
    }
}
