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
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.configmanagement.CassandraConfigurationOverlay;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationManager;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationManagerException;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationOverlaySnapshot;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationProviderUnavailableException;
import org.apache.cassandra.sidecar.modules.ConfigurationManagementModule;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link ConfigurationGetHandler}
 */
@ExtendWith(VertxExtension.class)
public class ConfigurationGetHandlerTest
{
    Vertx vertx;
    Server server;
    ConfigurationManager mockConfigurationManager = mock(ConfigurationManager.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Module testOverride = Modules.override(new TestModule()).with(new ConfigurationGetHandlerTestModule());
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
    void testGetEffectiveConfiguration(VertxTestContext context)
    {
        Instant lastModified = Instant.parse("2026-06-01T12:00:00Z");
        JsonObject yamlConfig = new JsonObject().put("cluster_name", "TestCluster").put("num_tokens", 16);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yamlConfig, Collections.singletonMap("-Xmx", "4g"));
        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(lastModified, overlay);

        when(mockConfigurationManager.getEffectiveConfiguration(any(InstanceMetadata.class))).thenReturn(snapshot);

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", "/api/v1/cassandra/configuration")
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(200);
                  JsonObject body = response.bodyAsJsonObject();
                  assertThat(body.getString("hash")).startsWith("sha256:");
                  assertThat(body.getString("lastModified")).isEqualTo(lastModified.toString());
                  JsonObject configuration = body.getJsonObject("configuration");
                  assertThat(configuration.getJsonObject("cassandraYaml").getString("cluster_name")).isEqualTo("TestCluster");
                  assertThat(configuration.getJsonObject("extraJvmOpts").getString("-Xmx")).isEqualTo("4g");
                  assertThat(response.getHeader("ETag")).isEqualTo(snapshot.hash());
                  context.completeNow();
              }));
    }

    @Test
    void testGetEffectiveConfigurationProviderUnavailable(VertxTestContext context)
    {
        when(mockConfigurationManager.getEffectiveConfiguration(any(InstanceMetadata.class)))
                .thenThrow(new ConfigurationProviderUnavailableException("Provider is down", null));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", "/api/v1/cassandra/configuration")
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(503);
                  context.completeNow();
              }));
    }

    @Test
    void testGetEffectiveConfigurationManagerError(VertxTestContext context)
    {
        when(mockConfigurationManager.getEffectiveConfiguration(any(InstanceMetadata.class)))
                .thenThrow(new ConfigurationManagerException("Internal error", null));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", "/api/v1/cassandra/configuration")
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(500);
                  context.completeNow();
              }));
    }

    class ConfigurationGetHandlerTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public ConfigurationManager configurationManager()
        {
            return mockConfigurationManager;
        }
    }
}
