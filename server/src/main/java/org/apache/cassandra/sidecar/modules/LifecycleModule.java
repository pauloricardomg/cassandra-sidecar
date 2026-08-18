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

package org.apache.cassandra.sidecar.modules;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.LifecycleInfoResponse;
import org.apache.cassandra.sidecar.config.ConfigurationManagementConfiguration;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.apache.cassandra.sidecar.config.LifecycleConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.handlers.LifecycleInfoHandler;
import org.apache.cassandra.sidecar.handlers.LifecycleUpdateHandler;
import org.apache.cassandra.sidecar.lifecycle.LifecycleProvider;
import org.apache.cassandra.sidecar.lifecycle.ProcessLifecycleProvider;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.jetbrains.annotations.NotNull;

/**
 * Provides the telemetry capability
 */
public class LifecycleModule extends AbstractModule
{
    @Provides
    @Singleton
    LifecycleProvider lifecycleProvider(SidecarConfiguration sidecarConfiguration)
    {
        LifecycleConfiguration lifecycleConfiguration = sidecarConfiguration.lifecycleConfiguration();
        if (!lifecycleConfiguration.enabled())
        {
            return getNoopProvider();
        }

        ParameterizedClassConfiguration providerClass = lifecycleConfiguration.lifecycleProvider();
        if (providerClass == null)
        {
            throw new ConfigurationException("Lifecycle management is enabled, but provider not set.");
        }

        if (providerClass.className().equalsIgnoreCase(ProcessLifecycleProvider.class.getName()))
        {
            validateBaseTemplateNotMaterializationTarget(sidecarConfiguration);
            Map<String, String> namedParams = providerClass.namedParameters();
            return new ProcessLifecycleProvider(namedParams != null ? namedParams : Collections.emptyMap());
        }

        throw new ConfigurationException("Unrecognized authorization provider " + providerClass.className() + " set");
    }

    /**
     * Fails fast at startup if configuration management is enabled and its base template resolves to the same
     * file the lifecycle provider materializes the effective configuration to
     * ({@code <cassandra_conf_dir>/cassandra.yaml}). Sharing that file would overwrite the base template with
     * previously merged overlays, so the two must be distinct. No-op when configuration management is disabled
     * or no base template is configured.
     */
    private static void validateBaseTemplateNotMaterializationTarget(SidecarConfiguration sidecarConfiguration)
    {
        ConfigurationManagementConfiguration cmConfig = sidecarConfiguration.configurationManagementConfiguration();
        if (cmConfig == null || !cmConfig.enabled()
            || cmConfig.templates() == null || cmConfig.templates().cassandraYaml() == null)
        {
            return;
        }

        java.nio.file.Path baseTemplate = java.nio.file.Path.of(cmConfig.templates().cassandraYaml());
        for (InstanceConfiguration instance : sidecarConfiguration.cassandraInstances())
        {
            String confDir = instance.lifecycleOptions().get(ProcessLifecycleProvider.OPT_CASSANDRA_CONF_DIR);
            if (confDir == null)
            {
                continue;
            }
            java.nio.file.Path target = java.nio.file.Path.of(confDir).resolve("cassandra.yaml");
            if (isSameFile(baseTemplate, target))
            {
                throw new ConfigurationException(
                        "Configuration base template '" + baseTemplate + "' resolves to the same file as the "
                        + "lifecycle materialization target '" + target + "' for instance " + instance.id()
                        + ". They must be different files; otherwise the base template would be overwritten with "
                        + "merged overlays. Point configuration_management.templates.cassandra_yaml at a separate file.");
            }
        }
    }

    private static boolean isSameFile(java.nio.file.Path a, java.nio.file.Path b)
    {
        try
        {
            if (Files.exists(a) && Files.exists(b))
            {
                return Files.isSameFile(a, b);
            }
        }
        catch (IOException e)
        {
            // fall back to normalized path comparison below
        }
        return a.toAbsolutePath().normalize().equals(b.toAbsolutePath().normalize());
    }

    private static @NotNull LifecycleProvider getNoopProvider()
    {
        return new LifecycleProvider()
        {
            @Override
            public void start(InstanceMetadata instance)
            {
                throw new UnsupportedOperationException("Lifecycle management is disabled. Cannot start instance " + instance.host());
            }

            @Override
            public void stop(InstanceMetadata instance)
            {
                throw new UnsupportedOperationException("Lifecycle management is disabled. Cannot stop instance " + instance.host());
            }

            @Override
            public boolean isRunning(InstanceMetadata instance)
            {
                throw new UnsupportedOperationException("Lifecycle management is disabled. Cannot check if instance " + instance.host() + " is running");
            }
        };
    }

    @PUT
    @Path(ApiEndpointsV1.LIFECYCLE_ROUTE)
    @Operation(summary = "Updates desired lifecycle state",
            description = "Updates the desired lifecycle state for a local Cassandra node. Valid states are 'RUNNING' or 'STOPPED'.")
    @APIResponse(description = "Desired lifecycle state updated successfully",
            responseCode = "202",
            content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = LifecycleInfoResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LifecycleUpdateRouteKey.class)
    VertxRoute lifecycleUpdateRoute(RouteBuilder.Factory factory,
                                    LifecycleUpdateHandler lifecycleUpdateHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(lifecycleUpdateHandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.LIFECYCLE_ROUTE)
    @Operation(summary = "Gets lifecycle information",
            description = "Returns the lifecycle information for a local Cassandra node")
    @APIResponse(description = "Lifecycle information retrieved successfully",
            responseCode = "200",
            content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = LifecycleInfoResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LifecycleInfoRouteKey.class)
    VertxRoute lifecycleInfoRoute(RouteBuilder.Factory factory,
                                    LifecycleInfoHandler lifecycleInfoHandler)
    {
        return factory.buildRouteWithHandler(lifecycleInfoHandler);
    }
}
