/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework;

import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.identity.PluginSubject;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;

import java.io.IOException;
import java.security.Principal;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_MULTI_TENANCY_ENABLED;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_ACTIVE_DEPROVISIONS_PER_TENANT;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_ACTIVE_PROVISIONS_PER_TENANT;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOWS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOW_STEPS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.REMOTE_METADATA_ENDPOINT;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.REMOTE_METADATA_REGION;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.REMOTE_METADATA_SERVICE_NAME;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.REMOTE_METADATA_TYPE;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.TASK_REQUEST_RETRY_DURATION;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.WORKFLOW_REQUEST_TIMEOUT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
        when(client.threadPool()).thenReturn(threadPool);

        environment = mock(Environment.class);
        settings = Settings.builder().build();
        when(environment.settings()).thenReturn(settings);

        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(
                FLOW_FRAMEWORK_ENABLED,
                MAX_WORKFLOWS,
                MAX_WORKFLOW_STEPS,
                WORKFLOW_REQUEST_TIMEOUT,
                TASK_REQUEST_RETRY_DURATION,
                FILTER_BY_BACKEND_ROLES,
                FLOW_FRAMEWORK_MULTI_TENANCY_ENABLED,
                MAX_ACTIVE_PROVISIONS_PER_TENANT,
                MAX_ACTIVE_DEPROVISIONS_PER_TENANT,
                REMOTE_METADATA_TYPE,
                REMOTE_METADATA_ENDPOINT,
                REMOTE_METADATA_REGION,
                REMOTE_METADATA_SERVICE_NAME
            )
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
                7,
                ffp.createComponents(client, clusterService, threadPool, null, null, null, environment, null, null, null, null).size()
            );
            assertEquals(9, ffp.getRestHandlers(settings, null, null, null, null, null, null).size());
            assertEquals(10, ffp.getActions().size());
            assertEquals(3, ffp.getExecutorBuilders(settings).size());
            assertEquals(16, ffp.getSettings().size());

            Collection<SystemIndexDescriptor> systemIndexDescriptors = ffp.getSystemIndexDescriptors(Settings.EMPTY);
            assertEquals(3, systemIndexDescriptors.size());
        }
    }

    public void testAssignSubjectGivesThePluginClientItsSubject() throws IOException {
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        AtomicInteger runs = new AtomicInteger();
        PluginSubject subject = new PluginSubject() {
            @Override
            public Principal getPrincipal() {
                return () -> "plugin:flow-framework";
            }

            @Override
            public <E extends Exception> void runAs(CheckedRunnable<E> runnable) throws E {
                runs.incrementAndGet();
                try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
                    runnable.run();
                }
            }
        };

        try (FlowFrameworkPlugin ffp = new FlowFrameworkPlugin()) {
            FlowFrameworkIndicesHandler indicesHandler = ffp.createComponents(
                client,
                clusterService,
                threadPool,
                null,
                null,
                null,
                environment,
                null,
                null,
                null,
                null
            )
                .stream()
                .filter(FlowFrameworkIndicesHandler.class::isInstance)
                .map(FlowFrameworkIndicesHandler.class::cast)
                .findFirst()
                .orElseThrow();
            ffp.assignSubject(subject);

            // The indices handler reaches the system indices only through the plugin's own client.
            AtomicReference<Exception> failure = new AtomicReference<>();
            indicesHandler.initConfigIndexIfAbsent(ActionListener.wrap(r -> {}, failure::set));

            assertNull("creating the index must not fail", failure.get());
            assertEquals("the create index action must run as the assigned subject", 1, runs.get());
            verify(client).execute(eq(CreateIndexAction.INSTANCE), any(CreateIndexRequest.class), any());
        }
    }
}
