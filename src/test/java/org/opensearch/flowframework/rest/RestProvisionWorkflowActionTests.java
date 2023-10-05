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
import org.opensearch.flowframework.FlowFrameworkPlugin;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.mockito.Mockito.mock;

public class RestProvisionWorkflowActionTests extends OpenSearchTestCase {

    private String invalidTemplate;
    private RestProvisionWorkflowAction provisionWorkflowRestAction;
    private String provisionInlineWorkflowPath;
    private String provisionSavedWorkflowPath;
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
        this.provisionWorkflowRestAction = new RestProvisionWorkflowAction();
        this.provisionInlineWorkflowPath = String.format(Locale.ROOT, "%s/%s", FlowFrameworkPlugin.WORKFLOWS_URI, "_provision");
        this.provisionSavedWorkflowPath = String.format(
            Locale.ROOT,
            "%s/{%s}/%s",
            FlowFrameworkPlugin.WORKFLOWS_URI,
            "workflow_id",
            "_provision"
        );
        this.nodeClient = mock(NodeClient.class);
    }

    public void testRestProvisionWorkflowActionName() {
        String name = provisionWorkflowRestAction.getName();
        assertEquals("provision_workflow_action", name);
    }

    public void testRestProvisiionWorkflowActionRoutes() {
        List<Route> routes = provisionWorkflowRestAction.routes();
        assertEquals(2, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals(RestRequest.Method.POST, routes.get(1).getMethod());
        assertEquals(this.provisionInlineWorkflowPath, routes.get(0).getPath());
        assertEquals(this.provisionSavedWorkflowPath, routes.get(1).getPath());
    }

    public void testNullWorkflowIdAndTemplate() throws IOException {

        // Request with no content or params
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionInlineWorkflowPath)
            .build();

        IOException ex = expectThrows(IOException.class, () -> { provisionWorkflowRestAction.prepareRequest(request, nodeClient); });
        assertEquals("workflow_id and template cannot be both null", ex.getMessage());
    }

    public void testInvalidProvisionInlineWorkflowRequest() throws IOException {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionInlineWorkflowPath)
            .withContent(new BytesArray(invalidTemplate), MediaTypeRegistry.JSON)
            .build();

        IOException ex = expectThrows(IOException.class, () -> { provisionWorkflowRestAction.prepareRequest(request, nodeClient); });
        assertEquals("An template object requires a name.", ex.getMessage());
    }

}
