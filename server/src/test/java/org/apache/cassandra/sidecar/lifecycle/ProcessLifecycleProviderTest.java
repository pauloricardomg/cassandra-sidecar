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

package org.apache.cassandra.sidecar.lifecycle;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.mockito.MockedStatic;

import static org.apache.cassandra.sidecar.lifecycle.ProcessLifecycleProvider.getPidFileLocation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ProcessLifecycleProvider}. These tests use mocking to simulate process behavior.
 * Actual process execution is tested by the {@link org.apache.cassandra.sidecar.lifecycle.ProcessLifecycleProviderIntegrationTest}
 */
public class ProcessLifecycleProviderTest
{
    @TempDir
    Path lifecycleStateDir;

    /**
     * A fake implementation of ProcessLifecycleProvider for testing purposes.
     * This class overrides the buildCassandraConfig method to return a mock configuration to avoid actual process execution.
     * Also, this simulates starting and stopping a process by creating and deleting a PID file.
     */
    class FakeProcessLifecycleProvider extends ProcessLifecycleProvider
    {
        public FakeProcessLifecycleProvider(Map<String, String> params)
        {
            super(params);
        }

        protected ProcessRuntimeConfiguration getRuntimeConfiguration(InstanceMetadata instance)
        {
            try
            {
                Process mockProcess = mock(Process.class);
                when(mockProcess.waitFor()).thenReturn(0);

                ProcessBuilder startMock = mock(ProcessBuilder.class);
                //when(startMock.start()).thenReturn(mockProcess);
                when(startMock.start()).then(invocation -> {
                    String pidFileLocation = getPidFileLocation(lifecycleStateDir.toString(), "localhost");
                    // create the pid file to simulate a started process
                    Path pidFile = Path.of(pidFileLocation);
                    Files.writeString(pidFile, "12345");
                    return mockProcess;
                });
                ProcessRuntimeConfiguration mockConfig = mock(ProcessRuntimeConfiguration.class);
                when(mockConfig.buildStartCommand(any(), any(), any())).thenReturn(startMock);

                ProcessBuilder stopMock = mock(ProcessBuilder.class);
                when(stopMock.start()).thenReturn(mockProcess);
                when(stopMock.start()).then(invocation -> {
                    String pidFileLocation = getPidFileLocation(lifecycleStateDir.toString(), "localhost");
                    // delete the pid file to simulate a stopped process
                    Path.of(pidFileLocation).toFile().delete();
                    return mockProcess;
                });
                when(mockConfig.instanceName()).thenReturn("localhost");
                return mockConfig;
            }
            catch (InterruptedException | IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    protected static final Logger LOG = LoggerFactory.getLogger(ProcessLifecycleProvider.class);


    @Test
    void testStartStopIsRunning() throws InterruptedException
    {
        try (MockedStatic<ProcessHandle> processHandleMock = mockStatic(ProcessHandle.class))
        {
            // Mock ProcessHandle.of to simulate process running state
            Optional<ProcessHandle> emptyHandle = Optional.empty();
            ProcessHandle mockHandle = mock(ProcessHandle.class);
            when(mockHandle.onExit()).thenReturn(CompletableFuture.supplyAsync(() -> {
                String pidFileLocation = getPidFileLocation(lifecycleStateDir.toString(), "localhost");
                // delete the pid file to simulate a stopped process
                File file = Path.of(pidFileLocation).toFile();
                file.delete();
                return null;
            }));
            Optional<ProcessHandle> presentHandle = Optional.of(mockHandle);
            processHandleMock.when(() -> ProcessHandle.of(12345L))
                             //.thenReturn(emptyHandle)  // First call - process not started yet to simulate delay while starting process
                             .thenReturn(presentHandle) // Second call - first successful check within start
                             .thenReturn(presentHandle) // Third call - second successful check within start
                             .thenReturn(presentHandle) // Fourth call - process is running check after start
                             .thenReturn(presentHandle) // Fourth call - process is running when stop is called
                             .thenReturn(emptyHandle);  // Subsequent calls - process not found


            // Create provider with temporary lifecycle state directory
            Map<String, String> params = Map.of(
            ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
            ProcessLifecycleProvider.OPT_CASSANDRA_HOME, "/default/cassandra/home"
            );
            FakeProcessLifecycleProvider provider = new FakeProcessLifecycleProvider(params);

            // Create mock instance metadata
            InstanceMetadata instance = mock(InstanceMetadata.class);
            when(instance.host()).thenReturn("localhost");
            when(instance.storageDir()).thenReturn("/custom/storage/dir");
            when(instance.lifecycleOptions()).thenReturn(Map.of());

            // Initially, instance should not be running (no PID file exists)
            String pidFileLocation = getPidFileLocation(lifecycleStateDir.toString(), "localhost");
            assertThat(Path.of(pidFileLocation)).doesNotExist();
            assertThat(provider.isRunning(instance)).isFalse();

            // Start the instance (first/second/third calls to isRunning happen within start)
            provider.start(instance);

            // After starting, instance should be running (PID file should exist)
            assertThat(Path.of(pidFileLocation)).exists();
            // Fourth call to isRunning should return true
            assertThat(provider.isRunning(instance)).isTrue();

            // Stop the instance (fourth/fifth call to isRunning happens within stop)
            //FIXME pid file not being created - use mock specific to this call
            provider.stop(instance);

            // After stopping, instance should not be running (PID file should be deleted)
            assertThat(Path.of(pidFileLocation)).doesNotExist();
            assertThat(provider.isRunning(instance)).isFalse();
        }
    }

    @Test
    void testBuildCassandraConfigWithCassandraHomeOverride()
    {
        Map<String, String> params = Map.of(
        ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, "/default/cassandra/home"
        );

        ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

        // Create mock instance metadata
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.host()).thenReturn("localhost");
        when(instance.storageDir()).thenReturn("/custom/storage/dir");

        Map<String, String> lifecycleOptions = Map.of(
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, "/instance/cassandra/home",
        ProcessLifecycleProvider.OPT_CASSANDRA_CONF_DIR, "/instance/conf/dir",
        ProcessLifecycleProvider.OPT_CASSANDRA_LOG_DIR, "/instance/log/dir"
        );
        when(instance.lifecycleOptions()).thenReturn(lifecycleOptions);

        ProcessRuntimeConfiguration config = provider.getRuntimeConfiguration(instance);

        // Verify the configuration was built correctly
        assertThat(config.instanceName()).isEqualTo("localhost");
        assertThat(config.cassandraHome()).isEqualTo(Path.of("/instance/cassandra/home"));
        assertThat(config.cassandraConfDir).isEqualTo(Path.of("/instance/conf/dir"));
        assertThat(config.cassandraLogDir).isEqualTo("/instance/log/dir");
        assertThat(config.storageDir).isEqualTo("/custom/storage/dir");
    }

    @Test
    void testBuildCassandraConfigWithDefaultCassandraHome()
    {
        Map<String, String> params = Map.of(
        ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, "/default/cassandra/home"
        );

        ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

        // Create mock instance metadata
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.host()).thenReturn("localhost");
        when(instance.storageDir()).thenReturn("/custom/storage/dir");

        // Lifecycle options without CASSANDRA_HOME override
        Map<String, String> lifecycleOptions = Map.of(
        ProcessLifecycleProvider.OPT_CASSANDRA_CONF_DIR, "/instance/conf/dir",
        ProcessLifecycleProvider.OPT_CASSANDRA_LOG_DIR, "/instance/log/dir"
        );
        when(instance.lifecycleOptions()).thenReturn(lifecycleOptions);

        ProcessRuntimeConfiguration config = provider.getRuntimeConfiguration(instance);

        // Verify the configuration uses default Cassandra home
        assertThat(config.cassandraHome()).isEqualTo(Path.of("/default/cassandra/home"));
    }

    @Test
    void testBuildStartCommand() throws IOException
    {
        // Create temporary files to simulate the required directories and files first
        Path tempCassandraHome = lifecycleStateDir.resolve("cassandra");
        Path tempBinDir = tempCassandraHome.resolve("bin");
        Path tempConfDir = lifecycleStateDir.resolve("conf");
        Files.createDirectories(tempBinDir);
        Files.createDirectories(tempConfDir);

        Path cassandraBin = tempBinDir.resolve("cassandra");
        Path cassandraYaml = tempConfDir.resolve("cassandra.yaml");
        Files.createFile(cassandraBin);
        Files.createFile(cassandraYaml);
        cassandraBin.toFile().setExecutable(true);

        // Use the temp directory for Cassandra home instead of hardcoded path
        Map<String, String> params = Map.of(
        ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, tempCassandraHome.toString()
        );

        ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

        // Create mock instance metadata without storage dir
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.host()).thenReturn("testhost");
        when(instance.storageDir()).thenReturn(null);

        Map<String, String> lifecycleOptions = Map.of(
        ProcessLifecycleProvider.OPT_CASSANDRA_CONF_DIR, tempConfDir.toString()
        );
        when(instance.lifecycleOptions()).thenReturn(lifecycleOptions);

        // Build the config and test the start command using provider helper methods
        ProcessRuntimeConfiguration testConfig = provider.getRuntimeConfiguration(instance);
        String pidFileLocation = provider.getPidFileLocation("testhost");
        String stdoutLocation = provider.getStdoutLocation("testhost");
        String stderrLocation = provider.getStderrLocation("testhost");

        ProcessBuilder processBuilder = testConfig.buildStartCommand(pidFileLocation, stdoutLocation, stderrLocation);

        // Verify command arguments (no storage dir override)
        List<String> command = processBuilder.command();
        assertThat(command).containsExactly(
        cassandraBin.toString(),
        "-p",
        pidFileLocation
        );

        // Verify environment variables
        Map<String, String> env = processBuilder.environment();
        assertThat(env.get("CASSANDRA_HOME")).isEqualTo(tempCassandraHome.toString());
        assertThat(env.get("CASSANDRA_CONF")).isEqualTo(tempConfDir.toString());
        assertThat(env.get("CASSANDRA_LOG_DIR")).isNull();
    }

    @Test
    void testBuildStopCommand() throws IOException
    {
        Long pid = 12345L;
        String stdoutFile = "/tmp/stop-cassandra.out";
        String stderrFile = "/tmp/stop-cassandra.err";

        ProcessBuilder pb = ProcessLifecycleProvider.buildStopCommand(pid, stdoutFile, stderrFile);

        // Verify command
        List<String> command = pb.command();
        assertThat(command).hasSize(2);
        assertThat(command.get(0)).isEqualTo("kill");
        assertThat(command.get(1)).isEqualTo(pid.toString());

        // Verify redirects are configured
        assertThat(pb.redirectOutput().type()).isEqualTo(ProcessBuilder.Redirect.Type.APPEND);
        assertThat(pb.redirectError().type()).isEqualTo(ProcessBuilder.Redirect.Type.APPEND);
    }

    @Test
    void testBuildStartCommandWithExtraJvmOptsAndEnvVars() throws IOException
    {
        // Create temporary files to simulate the required directories and files first
        Path tempCassandraHome = lifecycleStateDir.resolve("cassandra");
        Path tempBinDir = tempCassandraHome.resolve("bin");
        Path tempConfDir = lifecycleStateDir.resolve("conf");
        Files.createDirectories(tempBinDir);
        Files.createDirectories(tempConfDir);

        Path cassandraBin = tempBinDir.resolve("cassandra");
        Path cassandraYaml = tempConfDir.resolve("cassandra.yaml");
        Files.createFile(cassandraBin);
        Files.createFile(cassandraYaml);
        cassandraBin.toFile().setExecutable(true);

        // Create provider with extra JVM options and environment variables
        Map<String, String> params = Map.of(
        ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, tempCassandraHome.toString(),
        "cassandra.max_queued_native_transport_requests", "1024",
        "env.JVM_OPTS", "-Xms1G -Xmx2G",
        "env.CUSTOM_VAR", "custom_value"
        );

        ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

        // Create mock instance metadata
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.host()).thenReturn("testhost");
        when(instance.storageDir()).thenReturn(null);

        Map<String, String> lifecycleOptions = Map.of(
        ProcessLifecycleProvider.OPT_CASSANDRA_CONF_DIR, tempConfDir.toString()
        );
        when(instance.lifecycleOptions()).thenReturn(lifecycleOptions);

        // Build the config and test the start command
        ProcessRuntimeConfiguration testConfig = provider.getRuntimeConfiguration(instance);
        String pidFileLocation = provider.getPidFileLocation("testhost");
        String stdoutLocation = provider.getStdoutLocation("testhost");
        String stderrLocation = provider.getStderrLocation("testhost");

        ProcessBuilder processBuilder = testConfig.buildStartCommand(pidFileLocation, stdoutLocation, stderrLocation);

        // Verify command includes JVM options as -D parameters
        List<String> command = processBuilder.command();
        assertThat(command).contains(cassandraBin.toString());
        assertThat(command).contains("-p");
        assertThat(command).contains(pidFileLocation);
        assertThat(command).contains("-Dcassandra.max_queued_native_transport_requests=1024");

        // Verify environment variables include both standard and extra vars
        Map<String, String> env = processBuilder.environment();
        assertThat(env.get("CASSANDRA_HOME")).isEqualTo(tempCassandraHome.toString());
        assertThat(env.get("CASSANDRA_CONF")).isEqualTo(tempConfDir.toString());
        assertThat(env.get("JVM_OPTS")).isEqualTo("-Xms1G -Xmx2G");
        assertThat(env.get("CUSTOM_VAR")).isEqualTo("custom_value");
    }
}
