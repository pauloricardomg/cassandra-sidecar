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

import java.nio.file.Path;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.config.ConfigurationManagementConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationManager;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationOverlaySnapshot;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationProvider;
import org.apache.cassandra.sidecar.configmanagement.FailurePolicy;
import org.apache.cassandra.sidecar.configmanagement.FileBasedConfigurationProvider;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.handlers.ConfigurationGetHandler;
import org.apache.cassandra.sidecar.handlers.ConfigurationPatchHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.headers.Header;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Provides the configuration management capability (CEP-62)
 */
public class ConfigurationManagementModule extends AbstractModule
{
    @Provides
    @Singleton
    ConfigurationManager configurationManager(SidecarConfiguration sidecarConfiguration)
    {
        ConfigurationManagementConfiguration config = sidecarConfiguration.configurationManagementConfiguration();

        Path configurationStore = Path.of(config.configurationStore());
        FailurePolicy failurePolicy = config.failurePolicy();

        ConfigurationProvider provider = createProvider(config, configurationStore);

        Path baseTemplatePath = config.templates().cassandraYaml() != null
                                ? Path.of(config.templates().cassandraYaml())
                                : null;

        return new ConfigurationManager(provider, baseTemplatePath, configurationStore, failurePolicy);
    }

    private ConfigurationProvider createProvider(ConfigurationManagementConfiguration config, Path configurationStore)
    {
        ParameterizedClassConfiguration providerConfig = config.provider();
        if (providerConfig == null || providerConfig.className() == null)
        {
            return new FileBasedConfigurationProvider(configurationStore);
        }

        String className = providerConfig.className();
        if (className.equals(FileBasedConfigurationProvider.class.getName()))
        {
            return new FileBasedConfigurationProvider(configurationStore);
        }

        throw new ConfigurationException("Unrecognized configuration provider: " + className);
    }

    @GET
    @jakarta.ws.rs.Path(ApiEndpointsV1.CASSANDRA_CONFIGURATION_ROUTE)
    @Operation(summary = "Gets effective Cassandra configuration",
            description = "Returns the effective configuration for a local Cassandra instance, " +
                          "computed by merging the base template with any applied overlays")
    @APIResponse(description = "Effective configuration retrieved successfully",
            responseCode = "200",
            headers = @Header(name = "ETag",
                    description = "SHA-256 hash of the effective configuration for optimistic concurrency control",
                    schema = @Schema(type = SchemaType.STRING)),
            content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ConfigurationOverlaySnapshot.class)))
    @APIResponse(responseCode = "500", description = "Configuration management error")
    @APIResponse(responseCode = "503", description = "Configuration provider is unavailable")
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraConfigurationGetRouteKey.class)
    VertxRoute cassandraConfigurationGetRoute(RouteBuilder.Factory factory,
                                              ConfigurationGetHandler handler)
    {
        return factory.buildRouteWithHandler(handler);
    }

    @PATCH
    @jakarta.ws.rs.Path(ApiEndpointsV1.CASSANDRA_CONFIGURATION_ROUTE)
    @Consumes("application/json-patch+json")
    @Operation(summary = "Patches Cassandra configuration overlay",
            description = "Applies RFC 6902 JSON Patch operations to the configuration overlay "
                        + "for a local Cassandra instance. Requires If-Match header with the current "
                        + "configuration hash for optimistic concurrency control.")
    @APIResponse(description = "Configuration patched successfully",
            responseCode = "200",
            headers = @Header(name = "ETag",
                    description = "SHA-256 hash of the updated effective configuration",
                    schema = @Schema(type = SchemaType.STRING)),
            content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ConfigurationOverlaySnapshot.class)))
    @APIResponse(responseCode = "400", description = "Invalid patch request")
    @APIResponse(responseCode = "409", description = "Configuration conflict - hash mismatch")
    @APIResponse(responseCode = "415", description = "Unsupported media type - requires application/json-patch+json")
    @APIResponse(responseCode = "422", description = "Unprocessable entity - unsupported patch operation")
    @APIResponse(responseCode = "428", description = "Precondition required - missing If-Match header")
    @APIResponse(responseCode = "500", description = "Configuration management error")
    @APIResponse(responseCode = "503", description = "Configuration provider is unavailable")
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraConfigurationPatchRouteKey.class)
    VertxRoute cassandraConfigurationPatchRoute(RouteBuilder.Factory factory,
                                                ConfigurationPatchHandler handler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(handler)
                      .build();
    }
}
