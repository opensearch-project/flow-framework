/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOWS_URI;
import static org.mockito.Mockito.mock;

public class RestProvisionWorkflowActionTests extends OpenSearchTestCase {

    private RestProvisionWorkflowAction provisionWorkflowRestAction;
    private String provisionWorkflowPath;
    private NodeClient nodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.provisionWorkflowRestAction = new RestProvisionWorkflowAction();
        this.provisionWorkflowPath = String.format(Locale.ROOT, "%s/{%s}/%s", WORKFLOWS_URI, "workflow_id", "_provision");
        this.nodeClient = mock(NodeClient.class);
    }

    public void testRestProvisionWorkflowActionName() {
        String name = provisionWorkflowRestAction.getName();
        assertEquals("provision_workflow_action", name);
    }

    public void testRestProvisiionWorkflowActionRoutes() {
        List<Route> routes = provisionWorkflowRestAction.routes();
        assertEquals(1, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals(this.provisionWorkflowPath, routes.get(0).getPath());
    }

    public void testNullWorkflowIdAndTemplate() throws IOException {

        // Request with no content or params
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionWorkflowPath)
            .build();

        IOException ex = expectThrows(IOException.class, () -> { provisionWorkflowRestAction.prepareRequest(request, nodeClient); });
        assertEquals("workflow_id cannot be null", ex.getMessage());
    }

    public void testInvalidRequestWithContent() throws IOException {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionWorkflowPath)
            .withContent(new BytesArray("request body"), MediaTypeRegistry.JSON)
            .build();

        IOException ex = expectThrows(IOException.class, () -> { provisionWorkflowRestAction.prepareRequest(request, nodeClient); });
        assertEquals("Invalid request format", ex.getMessage());
    }

}
