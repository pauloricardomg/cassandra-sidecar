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

package org.apache.cassandra.sidecar.config.yaml;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.ConfigurationManagementConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.TemplatesConfiguration;
import org.apache.cassandra.sidecar.configmanagement.FailurePolicy;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration for Cassandra configuration management via Sidecar (CEP-62)
 */
public class ConfigurationManagementConfigurationImpl implements ConfigurationManagementConfiguration
{
    private static final boolean DEFAULT_ENABLED = false;
    private static final String DEFAULT_CONFIGURATION_STORE = "/var/lib/cassandra-sidecar/configuration-store";
    private static final FailurePolicy DEFAULT_FAILURE_POLICY = FailurePolicy.CACHED_READ_ONLY;

    protected final boolean enabled;
    protected final String configurationStore;
    protected final TemplatesConfiguration templates;
    protected final FailurePolicy failurePolicy;
    @Nullable
    protected final ParameterizedClassConfiguration provider;

    public ConfigurationManagementConfigurationImpl()
    {
        this(DEFAULT_ENABLED, DEFAULT_CONFIGURATION_STORE, new TemplatesConfigurationImpl(),
             DEFAULT_FAILURE_POLICY, null);
    }

    @JsonCreator
    public ConfigurationManagementConfigurationImpl(@JsonProperty("enabled") Boolean enabled,
                                                    @JsonProperty("configuration_store") String configurationStore,
                                                    @JsonProperty("templates") TemplatesConfiguration templates,
                                                    @JsonProperty("failure_policy") FailurePolicy failurePolicy,
                                                    @JsonProperty("provider") ParameterizedClassConfiguration provider)
    {
        this.enabled = enabled != null ? enabled : DEFAULT_ENABLED;
        this.configurationStore = configurationStore != null ? configurationStore : DEFAULT_CONFIGURATION_STORE;
        this.templates = templates != null ? templates : new TemplatesConfigurationImpl();
        this.failurePolicy = failurePolicy != null ? failurePolicy : DEFAULT_FAILURE_POLICY;
        this.provider = provider;
    }

    @Override
    @JsonProperty("enabled")
    public boolean enabled()
    {
        return enabled;
    }

    @Override
    @JsonProperty("configuration_store")
    public String configurationStore()
    {
        return configurationStore;
    }

    @Override
    @JsonProperty("templates")
    public TemplatesConfiguration templates()
    {
        return templates;
    }

    @Override
    @JsonProperty("failure_policy")
    public FailurePolicy failurePolicy()
    {
        return failurePolicy;
    }

    @Override
    @JsonProperty("provider")
    @Nullable
    public ParameterizedClassConfiguration provider()
    {
        return provider;
    }
}
