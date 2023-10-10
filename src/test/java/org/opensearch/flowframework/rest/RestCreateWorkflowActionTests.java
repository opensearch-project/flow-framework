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

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.mockito.Mockito.mock;

public class RestCreateWorkflowActionTests extends OpenSearchTestCase {

    private String invalidTemplate;
    private RestCreateWorkflowAction createWorkflowRestAction;
    private String createWorkflowPath;
    private String updateWorkflowPath;
    private NodeClient nodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Invalid template configuration, missing name field
        this.invalidTemplate = "{\"description\":\"description\","
            + "\"use_case\":\"use case\","
            + "\"operations\":[\"operation\"],"
            + "\"version\":{\"template\":\"1.0.0\",\"compatibility\":[\"2.0.0\",\"3.0.0\"]},"
            + "\"user_inputs\":{\"userKey\":\"userValue\",\"userMapKey\":{\"nestedKey\":\"nestedValue\"}},"
            + "\"workflows\":{\"workflow\":{\"user_params\":{\"key\":\"value\"},\"nodes\":[{\"id\":\"A\",\"type\":\"a-type\",\"inputs\":{\"foo\":\"bar\"}},{\"id\":\"B\",\"type\":\"b-type\",\"inputs\":{\"baz\":\"qux\"}}],\"edges\":[{\"source\":\"A\",\"dest\":\"B\"}]}}}";
        this.createWorkflowRestAction = new RestCreateWorkflowAction();
        this.createWorkflowPath = String.format(Locale.ROOT, "%s", WORKFLOW_URI);
        this.updateWorkflowPath = String.format(Locale.ROOT, "%s/{%s}", WORKFLOW_URI, "workflow_id");
        this.nodeClient = mock(NodeClient.class);
    }

    public void testRestCreateWorkflowActionName() {
        String name = createWorkflowRestAction.getName();
        assertEquals("create_workflow_action", name);
    }

    public void testRestCreateWorkflowActionRoutes() {
        List<Route> routes = createWorkflowRestAction.routes();
        assertEquals(2, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals(RestRequest.Method.PUT, routes.get(1).getMethod());
        assertEquals(this.createWorkflowPath, routes.get(0).getPath());
        assertEquals(this.updateWorkflowPath, routes.get(1).getPath());

    }

    public void testInvalidCreateWorkflowRequest() throws IOException {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.createWorkflowPath)
            .withContent(new BytesArray(invalidTemplate), MediaTypeRegistry.JSON)
            .build();

        IOException ex = expectThrows(IOException.class, () -> { createWorkflowRestAction.prepareRequest(request, nodeClient); });
        assertEquals("An template object requires a name.", ex.getMessage());
    }
}
