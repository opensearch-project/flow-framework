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
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class WorkflowRequestResponseTests extends OpenSearchTestCase {

    private Template template;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        List<String> operations = List.of("operation");
        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        List<WorkflowNode> nodes = List.of(nodeA, nodeB);
        List<WorkflowEdge> edges = List.of(edgeAB);
        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, edges);

        this.template = new Template(
            "test",
            "description",
            "use case",
            operations,
            templateVersion,
            compatibilityVersions,
            Map.of("workflow", workflow),
            Map.of("outputKey", "outputValue"),
            Map.of("resourceKey", "resourceValue")
        );
    }

    public void testNullIdWorkflowRequest() throws IOException {
        WorkflowRequest nullIdRequest = new WorkflowRequest(null, template);
        assertNull(nullIdRequest.getWorkflowId());
        assertEquals(template, nullIdRequest.getTemplate());
        assertNull(nullIdRequest.validate());

        BytesStreamOutput out = new BytesStreamOutput();
        nullIdRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(nullIdRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(nullIdRequest.getTemplate().toJson(), streamInputRequest.getTemplate().toJson());
    }

    public void testNullTemplateWorkflowRequest() throws IOException {
        WorkflowRequest nullTemplateRequest = new WorkflowRequest("123", null);
        assertNotNull(nullTemplateRequest.getWorkflowId());
        assertNull(nullTemplateRequest.getTemplate());
        assertNull(nullTemplateRequest.validate());

        BytesStreamOutput out = new BytesStreamOutput();
        nullTemplateRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(nullTemplateRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(nullTemplateRequest.getTemplate(), streamInputRequest.getTemplate());
    }

    public void testWorkflowRequest() throws IOException {
        WorkflowRequest workflowRequest = new WorkflowRequest("123", template);
        assertNotNull(workflowRequest.getWorkflowId());
        assertEquals(template, workflowRequest.getTemplate());
        assertNull(workflowRequest.validate());

        BytesStreamOutput out = new BytesStreamOutput();
        workflowRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(workflowRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(workflowRequest.getTemplate().toJson(), streamInputRequest.getTemplate().toJson());

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
