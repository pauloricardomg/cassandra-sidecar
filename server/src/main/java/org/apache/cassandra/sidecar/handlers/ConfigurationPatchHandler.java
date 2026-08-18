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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationConflictException;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationManager;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationManagerException;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchException;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchOperation;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationProviderUnavailableException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handles {@code PATCH /api/v1/cassandra/configuration} requests for applying RFC 6902
 * JSON Patch operations to the Cassandra configuration overlay.
 */
@Singleton
public class ConfigurationPatchHandler
extends AbstractHandler<ConfigurationPatchHandler.PatchRequest> implements AccessProtected
{
    static final String JSON_PATCH_CONTENT_TYPE = "application/json-patch+json";

    private final ConfigurationManager configurationManager;

    @Inject
    public ConfigurationPatchHandler(InstanceMetadataFetcher metadataFetcher,
                                     ExecutorPools executorPools,
                                     ConfigurationManager configurationManager)
    {
        super(metadataFetcher, executorPools, null);
        this.configurationManager = configurationManager;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.MODIFY_CONFIGURATION.toAuthorization());
    }

    @Override
    protected PatchRequest extractParamsOrThrow(RoutingContext context)
    {
        String contentType = context.request().getHeader("Content-Type");
        if (contentType == null || !contentType.startsWith(JSON_PATCH_CONTENT_TYPE))
        {
            throw wrapHttpException(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE,
                                    "Content-Type must be " + JSON_PATCH_CONTENT_TYPE);
        }

        String ifMatch = context.request().getHeader("If-Match");
        if (ifMatch == null || ifMatch.isEmpty())
        {
            throw wrapHttpException(HttpResponseStatus.PRECONDITION_REQUIRED, "If-Match header is required");
        }

        JsonArray body;
        try
        {
            body = context.body().asJsonArray();
        }
        catch (Exception e)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Request body must be a JSON array", e);
        }

        if (body == null || body.isEmpty())
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Request body must be a non-empty JSON array of patch operations");
        }

        List<ConfigurationPatchOperation> operations = new ArrayList<>(body.size());
        for (int i = 0; i < body.size(); i++)
        {
            JsonObject entry = body.getJsonObject(i);
            if (entry == null)
            {
                throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                        "Patch operation at index " + i + " is not a valid JSON object");
            }

            String opStr = entry.getString("op");
            if (opStr == null)
            {
                throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                        "Patch operation at index " + i + " is missing required 'op' field");
            }

            ConfigurationPatchOperation.Op op;
            try
            {
                op = ConfigurationPatchOperation.Op.valueOf(opStr.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw wrapHttpException(HttpResponseStatus.UNPROCESSABLE_ENTITY,
                                       "Unsupported patch operation: '" + opStr + "'");
            }

            String path = entry.getString("path");
            if (path == null)
            {
                throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                        "Patch operation at index " + i + " is missing required 'path' field");
            }

            Object value = entry.getValue("value");
            operations.add(new ConfigurationPatchOperation(op, path, value));
        }

        return new PatchRequest(ifMatch, operations);
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  PatchRequest request)
    {
        executorPools.service()
                     .executeBlocking(() -> {
                         InstanceMetadata instance = metadataFetcher.instance(host);
                         return configurationManager.patchConfiguration(instance,
                                                                        request.expectedHash,
                                                                        request.operations);
                     })
                     .onSuccess(snapshot -> {
                         context.response().putHeader("ETag", snapshot.hash());
                         context.json(snapshot.toJson());
                     })
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    @Override
    protected void processFailure(Throwable cause, RoutingContext context, String host,
                                  SocketAddress remoteAddress, PatchRequest request)
    {
        if (cause instanceof ConfigurationConflictException)
        {
            context.response().putHeader("ETag", ((ConfigurationConflictException) cause).actualHash());
        }
        super.processFailure(cause, context, host, remoteAddress, request);
    }

    @Override
    protected HttpException determineHttpException(Throwable cause)
    {
        if (cause instanceof ConfigurationPatchException)
        {
            return wrapHttpException(HttpResponseStatus.BAD_REQUEST, cause.getMessage(), cause);
        }

        if (cause instanceof ConfigurationConflictException)
        {
            return wrapHttpException(HttpResponseStatus.CONFLICT, cause.getMessage(), cause);
        }

        if (cause instanceof ConfigurationProviderUnavailableException)
        {
            return wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE, cause.getMessage(), cause);
        }

        if (cause instanceof ConfigurationManagerException)
        {
            return wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.getMessage(), cause);
        }

        return super.determineHttpException(cause);
    }

    static class PatchRequest
    {
        final String expectedHash;
        final List<ConfigurationPatchOperation> operations;

        PatchRequest(String expectedHash, List<ConfigurationPatchOperation> operations)
        {
            this.expectedHash = expectedHash;
            this.operations = operations;
        }
    }
}
