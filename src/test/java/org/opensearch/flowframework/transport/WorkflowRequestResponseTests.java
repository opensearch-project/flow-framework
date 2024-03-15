/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class WorkflowRequestResponseTests extends OpenSearchTestCase {

    private Template template;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Collections.emptyMap(), Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Collections.emptyMap(), Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        List<WorkflowNode> nodes = List.of(nodeA, nodeB);
        List<WorkflowEdge> edges = List.of(edgeAB);
        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, edges);

        this.template = new Template(
            "test",
            "description",
            "use case",
            templateVersion,
            compatibilityVersions,
            Map.of("workflow", workflow),
            Collections.emptyMap(),
            TestHelpers.randomUser(),
            null,
            null,
            null
        );
    }

    public void testNullIdWorkflowRequest() throws IOException {
        WorkflowRequest nullIdRequest = new WorkflowRequest(null, template);
        assertNull(nullIdRequest.getWorkflowId());
        assertEquals(template, nullIdRequest.getTemplate());
        assertNull(nullIdRequest.validate());
        assertFalse(nullIdRequest.isProvision());
        assertTrue(nullIdRequest.getParams().isEmpty());

        BytesStreamOutput out = new BytesStreamOutput();
        nullIdRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(nullIdRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(nullIdRequest.getTemplate().toString(), streamInputRequest.getTemplate().toString());
        assertNull(nullIdRequest.validate());
        assertFalse(nullIdRequest.isProvision());
        assertTrue(nullIdRequest.getParams().isEmpty());
    }

    public void testNullTemplateWorkflowRequest() throws IOException {
        WorkflowRequest nullTemplateRequest = new WorkflowRequest("123", null);
        assertNotNull(nullTemplateRequest.getWorkflowId());
        assertNull(nullTemplateRequest.getTemplate());
        assertNull(nullTemplateRequest.validate());
        assertFalse(nullTemplateRequest.isProvision());
        assertTrue(nullTemplateRequest.getParams().isEmpty());

        BytesStreamOutput out = new BytesStreamOutput();
        nullTemplateRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(nullTemplateRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(nullTemplateRequest.getTemplate(), streamInputRequest.getTemplate());
        assertNull(nullTemplateRequest.validate());
        assertFalse(nullTemplateRequest.isProvision());
        assertTrue(nullTemplateRequest.getParams().isEmpty());
    }

    public void testWorkflowRequest() throws IOException {
        WorkflowRequest workflowRequest = new WorkflowRequest("123", template);
        assertNotNull(workflowRequest.getWorkflowId());
        assertEquals(template, workflowRequest.getTemplate());
        assertNull(workflowRequest.validate());
        assertFalse(workflowRequest.isProvision());
        assertTrue(workflowRequest.getParams().isEmpty());

        BytesStreamOutput out = new BytesStreamOutput();
        workflowRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(workflowRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(workflowRequest.getTemplate().toString(), streamInputRequest.getTemplate().toString());
        assertNull(workflowRequest.validate());
        assertFalse(workflowRequest.isProvision());
        assertTrue(workflowRequest.getParams().isEmpty());
    }

    public void testWorkflowRequestWithParams() throws IOException {
        WorkflowRequest workflowRequest = new WorkflowRequest("123", template, Map.of("foo", "bar"));
        assertNotNull(workflowRequest.getWorkflowId());
        assertEquals(template, workflowRequest.getTemplate());
        assertNull(workflowRequest.validate());
        assertTrue(workflowRequest.isProvision());
        assertEquals("bar", workflowRequest.getParams().get("foo"));

        BytesStreamOutput out = new BytesStreamOutput();
        workflowRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(workflowRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(workflowRequest.getTemplate().toString(), streamInputRequest.getTemplate().toString());
        assertNull(workflowRequest.validate());
        assertTrue(workflowRequest.isProvision());
        assertEquals("bar", workflowRequest.getParams().get("foo"));
    }

    public void testWorkflowRequestWithUseCase() throws IOException {
        WorkflowRequest workflowRequest = new WorkflowRequest("123", template, "cohere-embedding_model_deploy", Collections.emptyMap());
        assertNotNull(workflowRequest.getWorkflowId());
        assertEquals(template, workflowRequest.getTemplate());
        assertNull(workflowRequest.validate());
        assertFalse(workflowRequest.isProvision());
        assertTrue(workflowRequest.getDefaultParams().isEmpty());
        assertEquals(workflowRequest.getUseCase(), "cohere-embedding_model_deploy");

        BytesStreamOutput out = new BytesStreamOutput();
        workflowRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(workflowRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(workflowRequest.getTemplate().toString(), streamInputRequest.getTemplate().toString());
        assertNull(workflowRequest.validate());
        assertFalse(workflowRequest.isProvision());
        assertTrue(workflowRequest.getDefaultParams().isEmpty());
        assertEquals(workflowRequest.getUseCase(), "cohere-embedding_model_deploy");
    }

    public void testWorkflowRequestWithUseCaseAndParamsInBody() throws IOException {
        WorkflowRequest workflowRequest = new WorkflowRequest("123", template, "cohere-embedding_model_deploy", Map.of("step", "model"));
        assertNotNull(workflowRequest.getWorkflowId());
        assertEquals(template, workflowRequest.getTemplate());
        assertNull(workflowRequest.validate());
        assertFalse(workflowRequest.isProvision());
        assertEquals(workflowRequest.getDefaultParams().get("step"), "model");

        BytesStreamOutput out = new BytesStreamOutput();
        workflowRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(workflowRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(workflowRequest.getTemplate().toString(), streamInputRequest.getTemplate().toString());
        assertNull(workflowRequest.validate());
        assertFalse(workflowRequest.isProvision());
        assertEquals(workflowRequest.getDefaultParams().get("step"), "model");

    }

    public void testWorkflowRequestWithParamsNoProvision() throws IOException {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new WorkflowRequest("123", template, new String[] { "all" }, false, Map.of("foo", "bar"), null, Collections.emptyMap())
        );
        assertEquals("Params may only be included when provisioning.", ex.getMessage());
    }

    public void testWorkflowResponse() throws IOException {
        WorkflowResponse response = new WorkflowResponse("123");
        assertEquals("123", response.getWorkflowId());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowResponse streamInputResponse = new WorkflowResponse(in);
        assertEquals(response.getWorkflowId(), streamInputResponse.getWorkflowId());

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertNotNull(builder);
        assertEquals("{\"workflow_id\":\"123\"}", builder.toString());
    }

}
