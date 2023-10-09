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
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlowFrameworkPluginTests extends OpenSearchTestCase {

    private Client client;
    private ThreadPool threadPool;
    private Settings settings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        when(client.admin()).thenReturn(mock(AdminClient.class));
        threadPool = new TestThreadPool(FlowFrameworkPluginTests.class.getName());
        settings = Settings.EMPTY;
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 500, TimeUnit.MILLISECONDS);
        super.tearDown();
    }

    public void testPlugin() throws IOException {
        try (FlowFrameworkPlugin ffp = new FlowFrameworkPlugin()) {
            assertEquals(3, ffp.createComponents(client, null, threadPool, null, null, null, null, null, null, null, null).size());
            assertEquals(2, ffp.getRestHandlers(null, null, null, null, null, null, null).size());
            assertEquals(2, ffp.getActions().size());
            assertEquals(1, ffp.getExecutorBuilders(settings).size());
        }
    }
}
