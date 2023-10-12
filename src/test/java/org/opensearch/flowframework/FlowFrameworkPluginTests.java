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
import org.opensearch.client.node.NodeClient;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlowFrameworkPluginTests extends OpenSearchTestCase {

    private Client client;
    private NodeClient nodeClient;

    private AdminClient adminClient;

    private ClusterAdminClient clusterAdminClient;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        threadPool = new TestThreadPool(FlowFrameworkPluginTests.class.getName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 500, TimeUnit.MILLISECONDS);
        super.tearDown();
    }

    public void testPlugin() throws IOException {
        try (FlowFrameworkPlugin ffp = new FlowFrameworkPlugin()) {
            assertEquals(2, ffp.createComponents(client, null, threadPool, null, null, null, null, null, null, null, null).size());
        }
    }
}
