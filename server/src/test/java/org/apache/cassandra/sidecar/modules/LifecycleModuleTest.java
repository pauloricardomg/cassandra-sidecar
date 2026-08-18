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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.sidecar.config.ConfigurationManagementConfiguration;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.apache.cassandra.sidecar.config.LifecycleConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.TemplatesConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.lifecycle.LifecycleProvider;
import org.apache.cassandra.sidecar.lifecycle.ProcessLifecycleProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LifecycleModule}, focusing on startup validation that the configuration management base
 * template does not collide with the lifecycle provider's materialization target.
 */
class LifecycleModuleTest
{
    @TempDir
    Path tempDir;

    @Test
    void testFailsFastWhenBaseTemplateEqualsMaterializationTarget()
    {
        // config management enabled with a base template that resolves to <conf_dir>/cassandra.yaml
        SidecarConfiguration config = mockConfig(true,
                                                 "/etc/cassandra/cassandra.yaml",
                                                 "/etc/cassandra",
                                                 Map.of());

        assertThatThrownBy(() -> new LifecycleModule().lifecycleProvider(config))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("same file")
                .hasMessageContaining("configuration_management.templates.cassandra_yaml");
    }

    @Test
    void testAllowsDistinctBaseTemplateAndTarget() throws IOException
    {
        Path stateDir = Files.createDirectories(tempDir.resolve("state"));
        Path cassandraHome = Files.createDirectories(tempDir.resolve("cassandra-home"));

        // base template is a separate file from <conf_dir>/cassandra.yaml
        SidecarConfiguration config = mockConfig(true,
                                                 "/etc/cassandra/cassandra.yaml.template",
                                                 "/etc/cassandra",
                                                 Map.of("state_dir", stateDir.toString(),
                                                        "cassandra_home", cassandraHome.toString()));

        LifecycleProvider provider = new LifecycleModule().lifecycleProvider(config);
        assertThat(provider).isInstanceOf(ProcessLifecycleProvider.class);
    }

    @Test
    void testConfigManagementDisabledSkipsCollisionCheck() throws IOException
    {
        Path stateDir = Files.createDirectories(tempDir.resolve("state"));
        Path cassandraHome = Files.createDirectories(tempDir.resolve("cassandra-home"));

        // config management disabled: even a colliding base template must not fail startup
        SidecarConfiguration config = mockConfig(false,
                                                 "/etc/cassandra/cassandra.yaml",
                                                 "/etc/cassandra",
                                                 Map.of("state_dir", stateDir.toString(),
                                                        "cassandra_home", cassandraHome.toString()));

        LifecycleProvider provider = new LifecycleModule().lifecycleProvider(config);
        assertThat(provider).isInstanceOf(ProcessLifecycleProvider.class);
    }

    private static SidecarConfiguration mockConfig(boolean configManagementEnabled,
                                                   String baseTemplatePath,
                                                   String cassandraConfDir,
                                                   Map<String, String> providerParams)
    {
        ParameterizedClassConfiguration providerClass = mock(ParameterizedClassConfiguration.class);
        when(providerClass.className()).thenReturn(ProcessLifecycleProvider.class.getName());
        when(providerClass.namedParameters()).thenReturn(providerParams);

        LifecycleConfiguration lifecycleConfig = mock(LifecycleConfiguration.class);
        when(lifecycleConfig.enabled()).thenReturn(true);
        when(lifecycleConfig.lifecycleProvider()).thenReturn(providerClass);

        TemplatesConfiguration templates = mock(TemplatesConfiguration.class);
        when(templates.cassandraYaml()).thenReturn(baseTemplatePath);

        ConfigurationManagementConfiguration cmConfig = mock(ConfigurationManagementConfiguration.class);
        when(cmConfig.enabled()).thenReturn(configManagementEnabled);
        when(cmConfig.templates()).thenReturn(templates);

        InstanceConfiguration instance = mock(InstanceConfiguration.class);
        when(instance.id()).thenReturn(1);
        when(instance.lifecycleOptions())
                .thenReturn(Map.of(ProcessLifecycleProvider.OPT_CASSANDRA_CONF_DIR, cassandraConfDir));

        SidecarConfiguration config = mock(SidecarConfiguration.class);
        when(config.lifecycleConfiguration()).thenReturn(lifecycleConfig);
        when(config.configurationManagementConfiguration()).thenReturn(cmConfig);
        when(config.cassandraInstances()).thenReturn(List.<InstanceConfiguration>of(instance));
        return config;
    }
}
