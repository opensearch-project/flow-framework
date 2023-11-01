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
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.List;
import java.util.Locale;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.mockito.Mockito.mock;

public class RestSearchWorkflowActionTests extends OpenSearchTestCase {
    private RestSearchWorkflowAction restSearchWorkflowAction;
    private String searchPath;
    private NodeClient nodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        this.searchPath = String.format(Locale.ROOT, "%s/%s", WORKFLOW_URI, "_search");
        this.restSearchWorkflowAction = new RestSearchWorkflowAction();
        this.nodeClient = mock(NodeClient.class);
    }

    public void testConstructor() {
        RestSearchWorkflowAction searchWorkflowAction = new RestSearchWorkflowAction();
        assertNotNull(searchWorkflowAction);
    }

    public void testRestSearchWorkflowActionName() {
        String name = restSearchWorkflowAction.getName();
        assertEquals("search_workflow_action", name);
    }

    public void testRestSearchWorkflowActionRoutes() {
        List<Route> routes = restSearchWorkflowAction.routes();
        assertNotNull(routes);
        assertEquals(2, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals(RestRequest.Method.GET, routes.get(1).getMethod());
        assertEquals(this.searchPath, routes.get(0).getPath());
        assertEquals(this.searchPath, routes.get(1).getPath());
    }

    public void testInvalidSearchRequest() {
        final String requestContent = "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"template\":\"1.0.0\"}}]}}}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(this.searchPath)
            .withContent(new BytesArray(requestContent), MediaTypeRegistry.JSON)
            .build();

        XContentParseException ex = expectThrows(XContentParseException.class, () -> {
            restSearchWorkflowAction.prepareRequest(request, nodeClient);
        });
        assertEquals("unknown named object category [org.opensearch.index.query.QueryBuilder]", ex.getMessage());
    }
}
