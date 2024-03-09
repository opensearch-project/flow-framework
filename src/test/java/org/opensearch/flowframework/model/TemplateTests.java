/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.Version;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TemplateTests extends OpenSearchTestCase {

    private String expectedTemplate =
        "{\"name\":\"test\",\"description\":\"a test template\",\"use_case\":\"test use case\",\"version\":{\"template\":\"1.2.3\",\"compatibility\":[\"4.5.6\",\"7.8.9\"]},"
            + "\"workflows\":{\"workflow\":{\"user_params\":{\"key\":\"value\"},\"nodes\":[{\"id\":\"A\",\"type\":\"a-type\",\"user_inputs\":{\"foo\":\"bar\"}},{\"id\":\"B\",\"type\":\"b-type\",\"user_inputs\":{\"baz\":\"qux\"}}],\"edges\":[{\"source\":\"A\",\"dest\":\"B\"}]}}}";

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testTemplate() throws IOException {
        Version templateVersion = Version.fromString("1.2.3");
        List<Version> compatibilityVersion = List.of(Version.fromString("4.5.6"), Version.fromString("7.8.9"));
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Collections.emptyMap(), Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Collections.emptyMap(), Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        List<WorkflowNode> nodes = List.of(nodeA, nodeB);
        List<WorkflowEdge> edges = List.of(edgeAB);
        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, edges);
        Map<String, Object> uiMetadata = null;

        Instant now = Instant.now();
        Template template = new Template(
            "test",
            "a test template",
            "test use case",
            templateVersion,
            compatibilityVersion,
            Map.of("workflow", workflow),
            uiMetadata,
            null,
            now,
            now,
            null
        );

        assertEquals("test", template.name());
        assertEquals("a test template", template.description());
        assertEquals("test use case", template.useCase());
        assertEquals(templateVersion, template.templateVersion());
        assertEquals(compatibilityVersion, template.compatibilityVersion());
        assertEquals(uiMetadata, template.getUiMetadata());
        Workflow wf = template.workflows().get("workflow");
        assertNotNull(wf);
        assertEquals(now, template.createdTime());
        assertEquals(now, template.lastUpdatedTime());
        assertNull(template.lastProvisionedTime());
        assertEquals("Workflow [userParams={key=value}, nodes=[A, B], edges=[A->B]]", wf.toString());

        String json = TemplateTestJsonUtil.parseToJson(template);

        Template templateX = Template.parse(json);
        assertEquals("test", templateX.name());
        assertEquals("a test template", templateX.description());
        assertEquals("test use case", templateX.useCase());
        assertEquals(templateVersion, templateX.templateVersion());
        assertEquals(compatibilityVersion, templateX.compatibilityVersion());
        assertEquals(uiMetadata, templateX.getUiMetadata());
        Workflow wfX = templateX.workflows().get("workflow");
        assertNotNull(wfX);
        assertEquals(now, template.createdTime());
        assertEquals(now, template.lastUpdatedTime());
        assertNull(template.lastProvisionedTime());
        assertEquals("Workflow [userParams={key=value}, nodes=[A, B], edges=[A->B]]", wfX.toString());
    }

    public void testExceptions() throws IOException {
        FlowFrameworkException e;

        String badTemplateField = expectedTemplate.replace("use_case", "badField");
        e = assertThrows(FlowFrameworkException.class, () -> Template.parse(badTemplateField));
        assertEquals("Unable to parse field [badField] in a template object.", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.getRestStatus());

        String badVersionField = expectedTemplate.replace("compatibility", "badField");
        e = assertThrows(FlowFrameworkException.class, () -> Template.parse(badVersionField));
        assertEquals("Unable to parse field [version] in a version object.", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.getRestStatus());
    }

    public void testStrings() throws IOException {
        Template t = Template.parse(expectedTemplate);
        assertTrue(t.toJson().contains("a test template"));
        assertTrue(t.toYaml().contains("a test template"));
        assertTrue(t.toString().contains("a test template"));
    }
}
