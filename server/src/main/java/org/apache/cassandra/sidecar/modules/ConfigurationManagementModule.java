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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.ConfigurationManagementConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationManager;
import org.apache.cassandra.sidecar.configmanagement.ConfigurationProvider;
import org.apache.cassandra.sidecar.configmanagement.FailurePolicy;
import org.apache.cassandra.sidecar.configmanagement.FileBasedConfigurationProvider;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

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
}
