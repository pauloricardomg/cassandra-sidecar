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
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.copyDirectoryRecursively;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.extractGzippedTarball;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.replacePlaceholdersInFileWithPattern;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for ProcessLifecycleProvider with configuration management enabled.
 * Verifies that managed configuration (cassandra.yaml materialization and extra JVM opts)
 * is applied when starting Cassandra via the lifecycle API.
 */
@ExtendWith(VertxExtension.class)
@Tag("heavy")
class ProcessLifecycleConfigManagementIntegrationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessLifecycleConfigManagementIntegrationTest.class);
    private static final String TEST_HOST = "localhost";
    private static final int TIMEOUT_SECONDS = 30;
    private static final String CONFIGURATION_ROUTE = "/api/v1/cassandra/configuration";
    private static final String LIFECYCLE_ROUTE = "/api/v1/cassandra/lifecycle";
    private static final String V2_SETTINGS_ROUTE = "/api/v2/cassandra/settings";
    private static final String JSON_PATCH_CONTENT_TYPE = "application/json-patch+json";

    static final String TARBALL_PATH = System.getProperty("cassandra.test.tarball_path");

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    private static Path tmpDir;

    private static Path lifecycleDir;
    private static Path cassandraConfDir;
    private static Server server;
    private static Vertx vertx;
    private static WebClient client;
    private static String sidecarDeploymentId;

    @BeforeAll
    static void setup() throws Exception
    {
        LOGGER.info("Created temporary directory for test: {}", tmpDir);

        // Install Cassandra from tarball
        Path cassandraInstallDir = tmpDir.resolve("opt");
        cassandraConfDir = Files.createDirectories(tmpDir.resolve("etc/cassandra"));
        Path cassandraHome = installCassandra(cassandraInstallDir, cassandraConfDir);
        Path cassandraStorageDir = Files.createDirectories(tmpDir.resolve("var/lib/cassandra"));
        Path cassandraLogDir = Files.createDirectories(tmpDir.resolve("var/log"));

        // Setup lifecycle and configuration management directories
        lifecycleDir = Files.createDirectories(tmpDir.resolve("var/lib/sidecar/lifecycle"));
        Path configurationStore = Files.createDirectories(tmpDir.resolve("var/lib/sidecar/config-store"));

        // The base template points to the ORIGINAL cassandra.yaml inside cassandraHome/conf
        // (never modified by lifecycle provider)
        Path cassandraYamlTemplate = cassandraHome.resolve("conf").resolve("cassandra.yaml");

        // Create sidecar configuration
        Path sidecarYaml = createSidecarYaml(cassandraHome, cassandraConfDir, cassandraStorageDir,
                                              cassandraLogDir, lifecycleDir, configurationStore,
                                              cassandraYamlTemplate);

        LOGGER.info("Starting sidecar with config: {}", sidecarYaml);
        Injector injector = Guice.createInjector(SidecarModules.all(sidecarYaml));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        client = WebClient.create(vertx);
        sidecarDeploymentId = server.start().toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
        LOGGER.info("Sidecar started on port {}", server.actualPort());
    }

    @AfterAll
    static void tearDown() throws ExecutionException, InterruptedException, TimeoutException
    {
        server.stop(sidecarDeploymentId).toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
        forceCassandraStop();
    }

    @Test
    void testStartWithManagedConfigurationAndRestart() throws Exception
    {
        LifecycleProviderIntegrationTester tester = new LifecycleProviderIntegrationTester(
                client, TEST_HOST, server.actualPort(),
                ProcessLifecycleConfigManagementIntegrationTest::forceCassandraStop);

        // 1. Patch configuration: set concurrent_writes=64 and add JVM opt
        String etag = getConfigurationEtag();
        JsonArray patchOps = new JsonArray()
                .add(new JsonObject().put("op", "add")
                                     .put("path", "/configuration/cassandraYaml/concurrent_writes")
                                     .put("value", 64))
                .add(new JsonObject().put("op", "add")
                                     .put("path", "/configuration/extraJvmOpts/-Dcassandra.gossip_settle_min_wait_ms")
                                     .put("value", "1000"));
        HttpResponse<Buffer> patchResponse = patchConfiguration(etag, patchOps);
        assertThat(patchResponse.statusCode()).isEqualTo(200);
        LOGGER.info("Patched configuration with concurrent_writes=64 and gossip_settle_min_wait_ms=1000");

        // 2. Start Cassandra via lifecycle API
        HttpResponse<Buffer> startResponse = putLifecycle("start");
        assertThat(startResponse.statusCode()).isIn(200, 202);

        // 3. Wait for start and CQL readiness
        try
        {
            tester.waitForCqlStatus("OK", LifecycleProviderIntegrationTester.TIMEOUT_SECONDS);
        }
        catch (Exception e)
        {
            Path stderrFile = lifecycleDir.resolve("start-cassandra-1.err");
            Path stdoutFile = lifecycleDir.resolve("start-cassandra-1.out");
            if (Files.exists(stderrFile))
            {
                LOGGER.error("Cassandra stderr:\n{}", Files.readString(stderrFile));
            }
            if (Files.exists(stdoutFile))
            {
                LOGGER.error("Cassandra stdout:\n{}", Files.readString(stdoutFile));
            }
            LOGGER.error("Materialized cassandra.yaml (first 50 lines):\n{}",
                         String.join("\n", Files.readAllLines(cassandraConfDir.resolve("cassandra.yaml")).subList(0,
                                 Math.min(50, Files.readAllLines(cassandraConfDir.resolve("cassandra.yaml")).size()))));
            throw e;
        }
        LOGGER.info("Cassandra started successfully with managed configuration");

        // 4. Verify concurrent_writes via /v2/settings
        verifySettingValue("concurrent_writes", "64");

        // 5. Verify JVM opt in process command line
        verifyJvmOptInCommandLine("cassandra.gossip_settle_min_wait_ms=1000");

        // 6. Verify cassandra.yaml.bkp was created
        assertThat(Files.exists(cassandraConfDir.resolve("cassandra.yaml.bkp")))
                .as("cassandra.yaml.bkp should exist after managed start")
                .isTrue();

        // 7. Patch configuration: change concurrent_writes to 128
        String newEtag = getConfigurationEtag();
        JsonArray updateOps = new JsonArray()
                .add(new JsonObject().put("op", "replace")
                                     .put("path", "/configuration/cassandraYaml/concurrent_writes")
                                     .put("value", 128));
        HttpResponse<Buffer> updateResponse = patchConfiguration(newEtag, updateOps);
        assertThat(updateResponse.statusCode()).isEqualTo(200);
        LOGGER.info("Updated configuration with concurrent_writes=128");

        // 8. Stop Cassandra
        putLifecycle("stop");
        waitForLifecycleLastUpdate("Instance has stopped", LifecycleProviderIntegrationTester.TIMEOUT_SECONDS);
        LOGGER.info("Cassandra stopped");

        // 9. Start Cassandra again
        putLifecycle("start");
        tester.waitForCqlStatus("OK", LifecycleProviderIntegrationTester.TIMEOUT_SECONDS);
        LOGGER.info("Cassandra restarted with updated configuration");

        // 10. Verify concurrent_writes updated to 128
        verifySettingValue("concurrent_writes", "128");

        // Cleanup: stop Cassandra
        putLifecycle("stop");
        waitForLifecycleLastUpdate("Instance has stopped", LifecycleProviderIntegrationTester.TIMEOUT_SECONDS);
    }

    private void verifySettingValue(String settingName, String expectedValue)
    {
        HttpResponse<Buffer> settingsResponse = getBlocking(
                client.get(server.actualPort(), TEST_HOST, V2_SETTINGS_ROUTE).send());
        assertThat(settingsResponse.statusCode()).isEqualTo(200);
        JsonObject settings = settingsResponse.bodyAsJsonObject().getJsonObject("nodeSettings");
        assertThat(settings.getString(settingName))
                .as("Expected %s=%s in /v2/settings", settingName, expectedValue)
                .isEqualTo(expectedValue);
    }

    private void verifyJvmOptInCommandLine(String expectedFragment)
    {
        Path pidFile = Path.of(ProcessLifecycleProvider.pidFileLocation(lifecycleDir.toString(), 1));
        Long pid = ProcessLifecycleProvider.readPidFromFile(pidFile);
        Optional<ProcessHandle> processHandle = ProcessHandle.of(pid);
        assertThat(processHandle).isPresent();

        Optional<String> cmdLine = ProcessLifecycleProvider.getCommandLinePlatformIndependent(processHandle.get());
        assertThat(cmdLine).isPresent();
        assertThat(cmdLine.get())
                .as("Expected JVM option '%s' in process command line", expectedFragment)
                .contains(expectedFragment);
    }

    private String getConfigurationEtag()
    {
        HttpResponse<Buffer> response = getBlocking(
                client.get(server.actualPort(), TEST_HOST, CONFIGURATION_ROUTE).send());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.getHeader("ETag");
    }

    private HttpResponse<Buffer> patchConfiguration(String ifMatch, JsonArray operations)
    {
        return getBlocking(client.patch(server.actualPort(), TEST_HOST, CONFIGURATION_ROUTE)
                                 .putHeader("Content-Type", JSON_PATCH_CONTENT_TYPE)
                                 .putHeader("If-Match", ifMatch)
                                 .sendBuffer(operations.toBuffer()));
    }

    private HttpResponse<Buffer> putLifecycle(String desiredState)
    {
        return getBlocking(client.put(server.actualPort(), TEST_HOST, LIFECYCLE_ROUTE)
                                 .sendBuffer(JsonObject.of("state", desiredState).toBuffer()));
    }

    private void waitForLifecycleLastUpdate(String expectedLastUpdate, int timeoutSeconds) throws TimeoutException
    {
        long startTime = System.nanoTime();
        while (true)
        {
            HttpResponse<Buffer> response = getBlocking(
                    client.get(server.actualPort(), TEST_HOST, LIFECYCLE_ROUTE).send());
            String lastUpdate = response.bodyAsJsonObject().getString("last_update");
            if (expectedLastUpdate.equals(lastUpdate))
            {
                return;
            }
            if (System.nanoTime() - startTime > SECONDS.toNanos(timeoutSeconds))
            {
                throw new TimeoutException("Expected lifecycle update '" + expectedLastUpdate +
                                           "' not reached after " + timeoutSeconds + " seconds. Last: " + lastUpdate);
            }
            try
            {
                Thread.sleep(2000);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private static Path installCassandra(Path installDir, Path confDir) throws IOException
    {
        if (TARBALL_PATH == null || TARBALL_PATH.isEmpty())
        {
            throw new IllegalStateException("System property 'cassandra.test.tarball_path' is not set");
        }
        Files.createDirectories(installDir);
        extractGzippedTarball(Path.of(TARBALL_PATH), installDir);
        java.io.File[] files = installDir.toFile().listFiles();
        assert files != null && files.length == 1 && files[0].isDirectory()
                : "Expected a single directory in " + installDir;
        Path cassandraHome = files[0].toPath();
        Path originalConfDir = cassandraHome.resolve("conf");
        copyDirectoryRecursively(originalConfDir, confDir);
        return cassandraHome;
    }

    private static Path createSidecarYaml(Path cassandraHome, Path confDir, Path storageDir,
                                           Path logDir, Path lifecycleDir, Path configStore,
                                           Path cassandraYamlTemplate) throws IOException, URISyntaxException
    {
        Path sidecarConfDir = Files.createDirectories(tmpDir.resolve("etc/sidecar"));
        URL templateUrl = ProcessLifecycleConfigManagementIntegrationTest.class
                .getResource("/config/sidecar_lifecycle_config_management.yaml.template");
        Path templatePath = Path.of(Objects.requireNonNull(templateUrl).toURI());
        Path sidecarYaml = sidecarConfDir.resolve("sidecar.yaml");
        replacePlaceholdersInFileWithPattern(templatePath,
                                            Map.of("cassandraHome", cassandraHome.toString(),
                                                   "lifecycleDir", lifecycleDir.toString(),
                                                   "cassandraConfDir", confDir.toString(),
                                                   "cassandraStorageDir", storageDir.toString(),
                                                   "cassandraLogDir", logDir.toString(),
                                                   "configurationStore", configStore.toString(),
                                                   "cassandraYamlTemplate", cassandraYamlTemplate.toString()),
                                            sidecarYaml);
        return sidecarYaml;
    }

    private static void forceCassandraStop()
    {
        Path pidFilePath = Path.of(ProcessLifecycleProvider.pidFileLocation(lifecycleDir.toString(), 1));
        if (!pidFilePath.toFile().exists())
        {
            LOGGER.info("No PID file exists, Cassandra already stopped.");
            return;
        }
        Long pid = ProcessLifecycleProvider.readPidFromFile(pidFilePath);
        try
        {
            Optional<ProcessHandle> processHandle = ProcessHandle.of(pid);
            if (processHandle.isPresent())
            {
                LOGGER.info("Killing Cassandra process with PID {}", pid);
                processHandle.get().onExit();
                processHandle.get().destroyForcibly();
                processHandle.get().onExit().get(TIMEOUT_SECONDS, SECONDS);
            }
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            LOGGER.error("Failed to kill Cassandra process with PID {}", pid, e);
            throw new RuntimeException("Failed to kill Cassandra process", e);
        }
    }
}
