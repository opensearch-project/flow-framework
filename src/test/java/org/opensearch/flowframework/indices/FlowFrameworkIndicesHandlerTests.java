/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class FlowFrameworkIndicesHandlerTests extends OpenSearchIntegTestCase {

    ClusterService clusterService;
    Client client;
    FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    @Before
    public void setUp() {
        clusterService = clusterService();
        client = client();
        flowFrameworkIndicesHandler = new FlowFrameworkIndicesHandler(clusterService, client);
    }

    public void testInitGlobalContextIndex() {
        ActionListener<Boolean> listener = ActionListener.wrap(r -> { assertTrue(r); }, e -> { throw new RuntimeException(e); });
        flowFrameworkIndicesHandler.initGlobalContextIndexIfAbsent(listener);
    }

}
