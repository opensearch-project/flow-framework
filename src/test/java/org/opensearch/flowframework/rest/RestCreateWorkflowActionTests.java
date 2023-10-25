/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.Version;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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

        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        List<WorkflowNode> nodes = List.of(nodeA, nodeB);
        List<WorkflowEdge> edges = List.of(edgeAB);
        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, edges);

        Template template = new Template(
            "test",
            "description",
            "use case",
            templateVersion,
            compatibilityVersions,
            Map.of("workflow", workflow)
        );

        // Invalid template configuration, wrong field name
        this.invalidTemplate = template.toJson().replace("use_case", "invalid");
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
        assertEquals("Unable to parse field [invalid] in a template object.", ex.getMessage());
    }
}
