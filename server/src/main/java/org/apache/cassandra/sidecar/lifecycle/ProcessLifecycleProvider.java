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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Manage the lifecycle of Cassandra instances running on local processes
 */
public class ProcessLifecycleProvider implements LifecycleProvider
{
    static final String OPT_CASSANDRA_HOME = "cassandra_home";
    static final String OPT_CASSANDRA_CONF_DIR = "cassandra_conf_dir";
    static final String OPT_CASSANDRA_LOG_DIR = "cassandra_log_dir";
    static final String OPT_STATE_DIR = "state_dir";

    protected static final Logger LOG = LoggerFactory.getLogger(ProcessLifecycleProvider.class);
    private static final long CASSANDRA_PROCESS_POLL_PERIOD_MS = Long.getLong("cassandra.sidecar.lifecycle.process.poll.period.ms", 5_000L);
    private static final long CASSANDRA_PROCESS_TIMEOUT_MS = Long.getLong("cassandra.sidecar.lifecycle.process.timeout.ms", 120_000L);

    private final String lifecycleDir;
    private final String defaultCassandraHome;
    private final Map<String, String> defaultJvmProperties = new HashMap<>();
    private final Map<String, String> defaultEnvVars = new HashMap<>();

    public ProcessLifecycleProvider(Map<String, String> params)
    {
        // Extract any JVM properties or environment variables from the params
        for (Map.Entry<String, String> entry : params.entrySet())
        {
            if (entry.getKey().startsWith("cassandra."))
            {
                defaultJvmProperties.put(entry.getKey(), entry.getValue());
            }
            else if (entry.getKey().startsWith("env."))
            {
                defaultEnvVars.put(entry.getKey().replaceAll("env.", ""), entry.getValue());
            }
        }
        this.lifecycleDir = params.get(OPT_STATE_DIR);
        this.defaultCassandraHome = params.get(OPT_CASSANDRA_HOME);
        if (lifecycleDir == null || lifecycleDir.isEmpty())
        {
            throw new ConfigurationException("Configuration property '" + OPT_STATE_DIR + "' must be set for ProcessLifecycleProvider");
        }
        if (defaultCassandraHome == null || defaultCassandraHome.isEmpty())
        {
            throw new ConfigurationException("Configuration property '" + OPT_CASSANDRA_HOME + "' must be set for ProcessLifecycleProvider");
        }
    }

    @Override
    public void start(InstanceMetadata instance)
    {
        if (isCassandraProcessRunning(instance))
        {
            LOG.info("Cassandra instance {} is already running.", instance.host());
            return;
        }
        startCassandra(instance);
    }

    @Override
    public void stop(InstanceMetadata instance)
    {
        if (!isCassandraProcessRunning(instance))
        {
            LOG.info("Cassandra instance {} is already stopped.", instance.host());
            return;
        }
        stopCassandra(instance);
    }

    @Override
    public boolean isRunning(InstanceMetadata instance)
    {
        return isCassandraProcessRunning(instance);
    }

    private void startCassandra(InstanceMetadata instance)
    {
        CassandraProcessConfiguration casCfg = buildCassandraConfig(instance);
        try
        {
            String stdoutLocation = getStdoutLocation(casCfg.instanceName());
            String stderrLocation = getStderrLocation(casCfg.instanceName());
            String pidFileLocation = getPidFileLocation(casCfg.instanceName());
            ProcessBuilder processBuilder = casCfg.buildStartCommand(pidFileLocation,
                                                                     stdoutLocation,
                                                                     stderrLocation);
            LOG.info("Starting Cassandra instance {} with command: {}", casCfg.instanceName(), processBuilder.command());
            Process process = processBuilder.start();
            process.waitFor();
            waitForPid(casCfg.instanceName(), getPidFileLocation(casCfg.instanceName()), true);
            if (isCassandraProcessRunning(instance))
            {
                LOG.info("Started Cassandra instance {} with PID {}", casCfg.instanceName(), readPidFromFile(Path.of(pidFileLocation)));
            }
            else
            {
                throw new RuntimeException("Failed to start Cassandra instance " + casCfg.instanceName() +
                                           ". Check stdout at " + stdoutLocation + " and stderr at " + stderrLocation);
            }
        }
        catch (IOException | InterruptedException e)
        {
            throw new RuntimeException("Failed to start Cassandra instance " + casCfg.instanceName() + " due to " + e.getMessage(), e);
        }
    }

    private void stopCassandra(InstanceMetadata instance)
    {
        CassandraProcessConfiguration casCfg = buildCassandraConfig(instance);
        try
        {
            String pidFileLocation = getPidFileLocation(casCfg.instanceName());
            ProcessBuilder processBuilder = casCfg.buildStopCommand(pidFileLocation,
                                                                    getStdoutLocation(casCfg.instanceName()),
                                                                    getStderrLocation(casCfg.instanceName()));
            LOG.info("Stopping Cassandra instance {} with command: {}", casCfg.instanceName(), processBuilder.command());
            Long pid = readPidFromFile(Path.of(pidFileLocation));
            Process process = processBuilder.start();
            process.waitFor();
            waitForPid(casCfg.instanceName(), pidFileLocation, false);
            if (isCassandraProcessRunning(instance))
            {
                throw new RuntimeException("Failed to stop Cassandra instance " + casCfg.instanceName() +
                                           ". Process is still running with PID " + pid);
            }
            else
            {
                LOG.info("Stopped Cassandra instance {} with PID {}. ", casCfg.instanceName(), pid);
            }
        }
        catch (IOException | InterruptedException e)
        {
            throw new RuntimeException("Failed to stop Cassandra instance " + casCfg.instanceName() + "due to " + e.getMessage(), e);
        }
    }


    /**
     * Wait for the Cassandra process with a given PID file to start or stop based on the 'started' flag
     */
    @VisibleForTesting
    public static void waitForPid(String instanceName, String pidFileLocation, boolean started) throws InterruptedException
    {
        Path pidFilePath = Path.of(pidFileLocation);
        if (!Files.exists(pidFilePath) || !Files.isReadable(pidFilePath))
        {
            LOG.debug("PID file does not exist or is not readable for instance {} at path {}", instanceName, pidFilePath);
            return;
        }
        long pid = readPidFromFile(pidFilePath);
        long elapsed = 0L;
        while (elapsed < CASSANDRA_PROCESS_TIMEOUT_MS && (started ? ProcessHandle.of(pid).isEmpty() : ProcessHandle.of(pid).isPresent()))
        {
            LOG.info("Waiting for Cassandra instance {} with PID {} to {}...", instanceName, pid, started ? "start" : "stop");
            Thread.sleep(CASSANDRA_PROCESS_POLL_PERIOD_MS);
            elapsed += CASSANDRA_PROCESS_POLL_PERIOD_MS;
        }
    }

    @VisibleForTesting
    protected CassandraProcessConfiguration buildCassandraConfig(InstanceMetadata instance)
    {
        String cassandraHome = Optional.ofNullable(instance.lifecycleOptions().get(OPT_CASSANDRA_HOME))
                                       .orElse(defaultCassandraHome);
        String cassandraConfDir = instance.lifecycleOptions().get(OPT_CASSANDRA_CONF_DIR);
        String cassandraLogDir = instance.lifecycleOptions().get(OPT_CASSANDRA_LOG_DIR);
        return new CassandraProcessConfiguration.Builder()
                                        .withHost(instance.host())
                                        .withCassandraHome(cassandraHome)
                                        .withCassandraConfDir(cassandraConfDir)
                                        .withCassandraLogDir(cassandraLogDir)
                                        .withStorageDir(instance.storageDir())
                                        .withJvmOptions(defaultJvmProperties)
                                        .withEnvVars(defaultEnvVars)
                                        .build();
    }

    public boolean isCassandraProcessRunning(InstanceMetadata instance)
    {
        Path pidFilePath = Path.of(getPidFileLocation(instance.host()));
        if (!Files.isRegularFile(pidFilePath) || !Files.isReadable(pidFilePath))
        {
            LOG.debug("PID file does not exist or is not readable for instance {} at path {}", instance.host(), pidFilePath);
            return false;
        }

        try
        {
            Long pid = readPidFromFile(pidFilePath);
            if (ProcessHandle.of(pid).isPresent())
            {
                LOG.debug("Cassandra instance {} is running with PID {}", instance.host(), pid);
                return true;
            }
            else
            {
                LOG.warn("PID file exists for instance {}, but process with PID {} is not running", instance.host(), pid);
                return false;
            }
        }
        catch (Exception e)
        {
            LOG.warn("Failed to read PID from file {} for instance {}: {}", pidFilePath, instance.host(), e.getMessage());
            return false;
        }
    }



    public static Long readPidFromFile(Path pidFilePath)
    {
        try
        {
            String pidFileContent = Files.readString(pidFilePath).trim();
            return Long.parseLong(pidFileContent);
        }
        catch (IOException | NumberFormatException e)
        {
            throw new RuntimeException("Failed to read PID from file: " + pidFilePath, e);
        }
    }

    protected String getPidFileLocation(String host)
    {
        return getPidFileLocation(lifecycleDir, host);
    }

    protected String getStdoutLocation(String instanceName)
    {
        return Paths.get(lifecycleDir, "cassandra-" + instanceName + ".out").toString();
    }

    protected String getStderrLocation(String instanceName)
    {
        return Paths.get(lifecycleDir, "cassandra-" + instanceName + ".err").toString();
    }

    @VisibleForTesting
    public static String getPidFileLocation(String lifecycleDir, String instanceName)
    {
        return Paths.get(lifecycleDir, "cassandra-" + instanceName + ".pid").toString();
    }
}
