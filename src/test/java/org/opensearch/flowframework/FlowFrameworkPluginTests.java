/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework;

import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOWS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.WORKFLOW_REQUEST_TIMEOUT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlowFrameworkPluginTests extends OpenSearchTestCase {

    private Client client;

    private AdminClient adminClient;

    private ClusterAdminClient clusterAdminClient;
    private ThreadPool threadPool;
    private Settings settings;
    private Environment environment;
    private ClusterSettings clusterSettings;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        threadPool = new TestThreadPool(FlowFrameworkPluginTests.class.getName());

        environment = mock(Environment.class);
        settings = Settings.builder().build();
        when(environment.settings()).thenReturn(settings);

        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(FLOW_FRAMEWORK_ENABLED, MAX_WORKFLOWS, WORKFLOW_REQUEST_TIMEOUT)
        ).collect(Collectors.toSet());
        clusterSettings = new ClusterSettings(settings, settingsSet);
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 500, TimeUnit.MILLISECONDS);
        super.tearDown();
    }

    public void testPlugin() throws IOException {
        try (FlowFrameworkPlugin ffp = new FlowFrameworkPlugin()) {
            assertEquals(
                3,
                ffp.createComponents(client, clusterService, threadPool, null, null, null, environment, null, null, null, null).size()
            );
            assertEquals(4, ffp.getRestHandlers(settings, null, null, null, null, null, null).size());
            assertEquals(4, ffp.getActions().size());
            assertEquals(1, ffp.getExecutorBuilders(settings).size());
            assertEquals(3, ffp.getSettings().size());
        }
    }
}
