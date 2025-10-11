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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents the configuration for a Cassandra process instance.
 */
public class ProcessRuntimeConfiguration
{
    static class Builder
    {
        private String host;
        private String cassandraHome;
        private String cassandraConfDir;
        private String cassandraLogDir;
        private String storageDir;
        private Map<String, String> jvmOpts;
        private Map<String, String> envVars;

        public Builder withHost(String host)
        {
            this.host = host;
            return this;
        }

        public Builder withCassandraHome(String cassandraHome)
        {
            this.cassandraHome = cassandraHome;
            return this;
        }

        public Builder withCassandraConfDir(String cassandraConfDir)
        {
            this.cassandraConfDir = cassandraConfDir;
            return this;
        }

        public Builder withCassandraLogDir(String cassandraLogDir)
        {
            this.cassandraLogDir = cassandraLogDir;
            return this;
        }

        public Builder withStorageDir(String storageDir)
        {
            this.storageDir = storageDir;
            return this;
        }

        public Builder withJvmOptions(Map<String, String> jvmOptions)
        {
            this.jvmOpts = jvmOptions;
            return this;
        }

        public Builder withEnvVars(Map<String, String> envVars)
        {
            this.envVars = envVars;
            return this;
        }

        public ProcessRuntimeConfiguration build()
        {
            ProcessRuntimeConfiguration casCfg = new ProcessRuntimeConfiguration(host, cassandraHome, cassandraConfDir, cassandraLogDir, storageDir);
            casCfg.extraEnvVars.putAll(envVars != null ? envVars : Map.of());
            casCfg.extraJvmOpts.putAll(jvmOpts != null ? jvmOpts : Map.of());
            return casCfg;
        }
    }

    @NotNull
    final String instanceName;
    @NotNull
    final Path cassandraHome;
    @NotNull
    final Path cassandraConfDir;
    @Nullable
    final String cassandraLogDir;
    @Nullable
    final String storageDir;
    final Map<String, String> extraJvmOpts = new HashMap<>();
    final Map<String, String> extraEnvVars = new HashMap<>();

    private ProcessRuntimeConfiguration(@NotNull String host, String cassandraHome, String cassandraConfDir,
                                        @Nullable String cassandraLogDir, @Nullable String storageDir)
    {
        this.instanceName = host;
        this.cassandraHome = Path.of(cassandraHome);
        this.cassandraConfDir = Path.of(cassandraConfDir);
        this.cassandraLogDir = cassandraLogDir;
        this.storageDir = storageDir;
    }

    public String instanceName()
    {
        return instanceName;
    }

    public Path cassandraHome()
    {
        return cassandraHome;
    }

    public String cassandraConf()
    {
        return cassandraConfDir.toString();
    }

    public Path cassandraBin()
    {
        return cassandraHome.resolve(Path.of("bin", "cassandra"));
    }

    public Path stopServerBin()
    {
        return cassandraHome.resolve(Path.of("bin", "stop-server"));
    }

    private Path cassandraYaml()
    {
        return cassandraConfDir.resolve(Path.of("cassandra.yaml"));
    }

    public void validateStart() throws IllegalArgumentException
    {
        // Check existence
        if (!Files.isDirectory(cassandraHome))
        {
            throw new IllegalArgumentException("Cassandra home does not exist or is not a directory: " + cassandraHome);
        }
        if (!Files.isDirectory(cassandraConfDir))
        {
            throw new IllegalArgumentException("Cassandra configuration directory does not exist or is not a directory: " + cassandraConfDir);
        }
        if (!Files.isRegularFile(cassandraYaml()))
        {
            throw new IllegalArgumentException("Cassandra YAML configuration file does not exist: " + cassandraYaml());
        }
        if (!Files.isRegularFile(cassandraBin()))
        {
            throw new IllegalArgumentException("Cassandra binary does not exist or is not a regular file: " + cassandraBin());
        }
        // Check permissions
        if (!Files.isExecutable(cassandraBin()))
        {
            throw new IllegalArgumentException("Cassandra binary is not executable: " + cassandraBin());
        }
        if (!Files.isReadable(cassandraConfDir))
        {
            throw new IllegalArgumentException("Cassandra configuration directory is not readable: " + cassandraConfDir);
        }
    }

    public ProcessBuilder buildStartCommand(String pidFileLocation, String stdoutFileLocation, String stderrFileLocation)
    {
        validateStart();

        List<String> startCassandraCmd = new ArrayList<>();
        startCassandraCmd.add(cassandraBin().toString());
        startCassandraCmd.add("-p");
        startCassandraCmd.add(pidFileLocation);
        for (Map.Entry<String, String> jvmOpt : extraJvmOpts.entrySet())
        {
            startCassandraCmd.add("-D" + jvmOpt.getKey() + "=" + jvmOpt.getValue());
        }

        // Override storage dir if present in sidecar configuration
        if (storageDir != null)
        {
            startCassandraCmd.add("-D");
            startCassandraCmd.add("cassandra.storagedir=" + storageDir);
        }

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(startCassandraCmd);

        // Set environment variables
        Map<String, String> env = processBuilder.environment();
        env.put("CASSANDRA_HOME", cassandraHome().toString());
        env.put("CASSANDRA_CONF", cassandraConf());
        env.putAll(extraEnvVars);

        // Only override CASSANDRA_LOG_DIR if it is set in the configuration
        if (cassandraLogDir != null)
        {
            env.put("CASSANDRA_LOG_DIR", cassandraLogDir);
        }

        // Redirect output to logs
        processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(stdoutFileLocation)));
        processBuilder.redirectError(ProcessBuilder.Redirect.appendTo(new File(stderrFileLocation)));

        // Set working directory
        processBuilder.directory(cassandraHome().toFile());
        return processBuilder;
    }

    public String toString()
    {
        return "ProcessRuntimeConfiguration{" +
               "instanceName='" + instanceName + '\'' +
               ", cassandraHome=" + cassandraHome +
               ", cassandraConfDir=" + cassandraConfDir +
               ", cassandraLogDir='" + cassandraLogDir + '\'' +
               ", storageDir='" + storageDir + '\'' +
               ", extraJvmOpts=" + extraJvmOpts +
               ", extraEnvVars=" + extraEnvVars +
               '}';
    }
}
