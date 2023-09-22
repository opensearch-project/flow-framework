/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.workflow.Workflow;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class WorkflowTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testWorkflow() throws IOException {
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        List<WorkflowNode> nodes = List.of(nodeA, nodeB);
        List<WorkflowEdge> edges = List.of(edgeAB);

        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, edges);
        assertEquals(Map.of("key", "value"), workflow.userParams());
        assertEquals(List.of(nodeA, nodeB), workflow.nodes());
        assertEquals(List.of(edgeAB), workflow.edges());

        String expectedJson = "{\"user_params\":{\"key\":\"value\"},"
            + "\"nodes\":[{\"id\":\"A\",\"type\":\"a-type\",\"inputs\":{\"foo\":\"bar\"}},"
            + "{\"id\":\"B\",\"type\":\"b-type\",\"inputs\":{\"baz\":\"qux\"}}],"
            + "\"edges\":[{\"source\":\"A\",\"dest\":\"B\"}]}";
        String json = TemplateTestJsonUtil.parseToJson(workflow);
        assertEquals(expectedJson, json);

        XContentParser parser = TemplateTestJsonUtil.jsonToParser(json);
        Workflow workflowX = Workflow.parse(parser);
        assertEquals(Map.of("key", "value"), workflowX.userParams());
        assertEquals(List.of(nodeA, nodeB), workflowX.nodes());
        assertEquals(List.of(edgeAB), workflowX.edges());
    }
}
