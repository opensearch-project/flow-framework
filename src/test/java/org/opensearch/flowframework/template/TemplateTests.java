/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.Version;
import org.opensearch.flowframework.workflow.Workflow;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TemplateTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testTemplate() throws IOException {
        Version templateVersion = Version.fromString("1.2.3");
        List<Version> compatibilityVersion = List.of(Version.fromString("4.5.6"), Version.fromString("7.8.9"));

        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        List<WorkflowNode> nodes = List.of(nodeA, nodeB);
        List<WorkflowEdge> edges = List.of(edgeAB);
        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, edges);

        Template template = new Template(
            "test",
            "a test template",
            "test use case",
            List.of("operation"),
            templateVersion,
            compatibilityVersion,
            Map.of("userKey", "userValue"),
            Map.of("workflow", workflow)
        );

        assertEquals("test", template.name());
        assertEquals("a test template", template.description());
        assertEquals("test use case", template.useCase());
        assertEquals(List.of("operation"), template.operations());
        assertEquals(templateVersion, template.templateVersion());
        assertEquals(compatibilityVersion, template.compatibilityVersion());
        assertEquals(Map.of("userKey", "userValue"), template.userInputs());
        Workflow wf = template.workflows().get("workflow");
        assertNotNull(wf);
        assertEquals("Workflow [userParams={key=value}, nodes=[A, B], edges=[A->B]]", wf.toString());

        String expectedJson = "{\"name\":\"test\",\"description\":\"a test template\",\"use_case\":\"test use case\","
            + "\"operations\":[\"operation\"],\"version\":{\"template\":\"1.2.3\",\"compatibility\":[\"4.5.6\",\"7.8.9\"]},"
            + "\"user_inputs\":{\"userKey\":\"userValue\"},\"workflows\":{\"workflow\":{\"user_params\":{\"key\":\"value\"},"
            + "\"nodes\":[{\"id\":\"A\",\"type\":\"a-type\",\"inputs\":{\"foo\":\"bar\"}},"
            + "{\"id\":\"B\",\"type\":\"b-type\",\"inputs\":{\"baz\":\"qux\"}}],"
            + "\"edges\":[{\"source\":\"A\",\"dest\":\"B\"}]}}}";
        String json = TemplateTestJsonUtil.parseToJson(template);
        assertEquals(expectedJson, json);

        Template templateX = TemplateParser.parseJsonToTemplate(json);
        assertEquals("test", templateX.name());
        assertEquals("a test template", templateX.description());
        assertEquals("test use case", templateX.useCase());
        assertEquals(List.of("operation"), templateX.operations());
        assertEquals(templateVersion, templateX.templateVersion());
        assertEquals(compatibilityVersion, templateX.compatibilityVersion());
        assertEquals(Map.of("userKey", "userValue"), templateX.userInputs());
        Workflow wfX = templateX.workflows().get("workflow");
        assertNotNull(wfX);
        assertEquals("Workflow [userParams={key=value}, nodes=[A, B], edges=[A->B]]", wfX.toString());
    }
}
