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

import java.util.Collections;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationManager;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationManagerException;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationProviderUnavailableException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handles {@code GET /api/v1/cassandra/configuration} requests for retrieving the effective
 * Cassandra configuration for a given instance.
 */
@Singleton
public class ConfigurationGetHandler extends AbstractHandler<Void> implements AccessProtected
{
    private final ConfigurationManager configurationManager;

    @Inject
    public ConfigurationGetHandler(InstanceMetadataFetcher metadataFetcher,
                                   ExecutorPools executorPools,
                                   ConfigurationManager configurationManager)
    {
        super(metadataFetcher, executorPools, null);
        this.configurationManager = configurationManager;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.READ_CONFIGURATION.toAuthorization());
    }

    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               Void request)
    {
        executorPools.service()
                     .executeBlocking(() -> {
                         InstanceMetadata instance = metadataFetcher.instance(host);
                         return configurationManager.getEffectiveConfiguration(instance);
                     })
                     .onSuccess(snapshot -> {
                         context.response().putHeader("ETag", snapshot.hash());
                         context.json(snapshot.toJson());
                     })
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }

    @Override
    protected HttpException determineHttpException(Throwable cause)
    {
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
}
