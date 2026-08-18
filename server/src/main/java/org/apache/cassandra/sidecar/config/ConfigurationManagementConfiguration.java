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

package org.apache.cassandra.sidecar.config;

import org.apache.cassandra.sidecar.configmanagement.FailurePolicy;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration for Cassandra configuration management via Sidecar (CEP-62)
 */
public interface ConfigurationManagementConfiguration
{
    /**
     * @return {@code true} if configuration management is enabled, {@code false} otherwise
     */
    boolean enabled();

    /**
     * @return the path to the local configuration store directory
     */
    String configurationStore();

    /**
     * @return the templates configuration containing base template paths
     */
    TemplatesConfiguration templates();

    /**
     * @return the failure policy to apply when the configuration provider is unavailable
     */
    FailurePolicy failurePolicy();

    /**
     * @return the configuration for the pluggable configuration provider, or {@code null} to use the
     *         default {@link org.apache.cassandra.sidecar.configmanagement.FileBasedConfigurationProvider}
     */
    @Nullable
    ParameterizedClassConfiguration provider();
}
