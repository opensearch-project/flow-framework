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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.flowframework.common.CommonValue.UPDATE_WORKFLOW_FIELDS;

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
            null,
            null,
            Collections.emptyList()
        );
    }

    public void testNullIdWorkflowRequest() throws IOException {
        WorkflowRequest nullIdRequest = new WorkflowRequest(null, template);
        assertNull(nullIdRequest.getWorkflowId());
        assertEquals(template, nullIdRequest.getTemplate());
        assertNull(nullIdRequest.validate());
        assertFalse(nullIdRequest.isProvision());
        assertFalse(nullIdRequest.isUpdateFields());
        assertTrue(nullIdRequest.getParams().isEmpty());

        BytesStreamOutput out = new BytesStreamOutput();
        nullIdRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(nullIdRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(nullIdRequest.getTemplate().toString(), streamInputRequest.getTemplate().toString());
        assertNull(streamInputRequest.validate());
        assertFalse(streamInputRequest.isProvision());
        assertFalse(streamInputRequest.isUpdateFields());
        assertTrue(streamInputRequest.getParams().isEmpty());
    }

    public void testNullTemplateWorkflowRequest() throws IOException {
        WorkflowRequest nullTemplateRequest = new WorkflowRequest("123", null);
        assertNotNull(nullTemplateRequest.getWorkflowId());
        assertNull(nullTemplateRequest.getTemplate());
        assertNull(nullTemplateRequest.validate());
        assertFalse(nullTemplateRequest.isProvision());
        assertFalse(nullTemplateRequest.isUpdateFields());
        assertTrue(nullTemplateRequest.getParams().isEmpty());

        BytesStreamOutput out = new BytesStreamOutput();
        nullTemplateRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(nullTemplateRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(nullTemplateRequest.getTemplate(), streamInputRequest.getTemplate());
        assertNull(streamInputRequest.validate());
        assertFalse(streamInputRequest.isProvision());
        assertFalse(streamInputRequest.isUpdateFields());
        assertTrue(streamInputRequest.getParams().isEmpty());
    }

    public void testWorkflowRequest() throws IOException {
        WorkflowRequest workflowRequest = new WorkflowRequest("123", template);
        assertNotNull(workflowRequest.getWorkflowId());
        assertEquals(template, workflowRequest.getTemplate());
        assertNull(workflowRequest.validate());
        assertFalse(workflowRequest.isProvision());
        assertFalse(workflowRequest.isUpdateFields());
        assertTrue(workflowRequest.getParams().isEmpty());

        BytesStreamOutput out = new BytesStreamOutput();
        workflowRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(workflowRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(workflowRequest.getTemplate().toString(), streamInputRequest.getTemplate().toString());
        assertNull(streamInputRequest.validate());
        assertFalse(streamInputRequest.isProvision());
        assertFalse(streamInputRequest.isUpdateFields());
        assertTrue(streamInputRequest.getParams().isEmpty());
    }

    public void testWorkflowRequestWithParams() throws IOException {
        WorkflowRequest workflowRequest = new WorkflowRequest("123", template, Map.of("foo", "bar"));
        assertNotNull(workflowRequest.getWorkflowId());
        assertEquals(template, workflowRequest.getTemplate());
        assertNull(workflowRequest.validate());
        assertTrue(workflowRequest.isProvision());
        assertFalse(workflowRequest.isUpdateFields());
        assertEquals("bar", workflowRequest.getParams().get("foo"));

        BytesStreamOutput out = new BytesStreamOutput();
        workflowRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(workflowRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(workflowRequest.getTemplate().toString(), streamInputRequest.getTemplate().toString());
        assertNull(streamInputRequest.validate());
        assertTrue(streamInputRequest.isProvision());
        assertFalse(streamInputRequest.isUpdateFields());
        assertEquals("bar", streamInputRequest.getParams().get("foo"));
    }

    public void testWorkflowRequestWithParamsNoProvision() throws IOException {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new WorkflowRequest("123", template, new String[] { "all" }, false, Map.of("foo", "bar"), false, null)
        );
        assertEquals("Params may only be included when provisioning.", ex.getMessage());
    }

    public void testWorkflowRequestWithOnlyUpdateParamNoProvision() throws IOException {
        WorkflowRequest workflowRequest = new WorkflowRequest(
            "123",
            template,
            new String[] { "all" },
            true,
            Map.of(UPDATE_WORKFLOW_FIELDS, "true"),
            false,
            null
        );
        assertNotNull(workflowRequest.getWorkflowId());
        assertEquals(template, workflowRequest.getTemplate());
        assertNull(workflowRequest.validate());
        assertFalse(workflowRequest.isProvision());
        assertTrue(workflowRequest.isUpdateFields());
        assertTrue(workflowRequest.getParams().isEmpty());

        BytesStreamOutput out = new BytesStreamOutput();
        workflowRequest.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest streamInputRequest = new WorkflowRequest(in);

        assertEquals(workflowRequest.getWorkflowId(), streamInputRequest.getWorkflowId());
        assertEquals(workflowRequest.getTemplate().toString(), streamInputRequest.getTemplate().toString());
        assertNull(streamInputRequest.validate());
        assertFalse(streamInputRequest.isProvision());
        assertTrue(streamInputRequest.isUpdateFields());
        assertTrue(streamInputRequest.getParams().isEmpty());
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

    public void testWorkflowResponseWithWaitForCompletionTimeOut() throws IOException {
        WorkflowState workFlowState = new WorkflowState(
            "123",
            "test",
            "PROVISIONING",
            "IN_PROGRESS",
            Instant.now(),
            Instant.now(),
            TestHelpers.randomUser(),
            Collections.emptyMap(),
            Collections.emptyList(),
            null,
            Collections.emptyList()
        );

        WorkflowResponse response = new WorkflowResponse("123", workFlowState);
        assertEquals("123", response.getWorkflowId());
        assertEquals("PROVISIONING", response.getWorkflowState().getState());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        WorkflowResponse streamInputResponse = new WorkflowResponse(in);

        assertEquals(response.getWorkflowId(), streamInputResponse.getWorkflowId());
        assertEquals(response.getWorkflowState().getState(), streamInputResponse.getWorkflowState().getState());

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertNotNull(builder);
        assertTrue(builder.toString().contains("\"workflow_id\":\"123\""));
        assertTrue(builder.toString().contains("\"state\":\"PROVISIONING\""));
    }

    public void testWorkflowRequestIndexIdTypeAndTimeout() throws IOException {
        TimeValue timeout = TimeValue.timeValueSeconds(15);

        WorkflowRequest request = new WorkflowRequest("123", template, Map.of("foo", "bar"), timeout);

        // Basic assertions
        assertEquals("123", request.getWorkflowId());
        assertEquals(template, request.getTemplate());
        assertNull(request.validate());
        assertTrue(request.isProvision());
        assertFalse(request.isUpdateFields());
        assertFalse(request.isReprovision());
        assertEquals("bar", request.getParams().get("foo"));
        assertEquals(timeout, request.getWaitForCompletionTimeout());

        // index/id/type metadata
        assertEquals(CommonValue.GLOBAL_CONTEXT_INDEX, request.index());
        assertEquals("123", request.id());
        assertEquals(CommonValue.WORKFLOW_RESOURCE_TYPE, request.type());

        // Round-trip through StreamInput/StreamOutput
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        WorkflowRequest requestFromStream = new WorkflowRequest(in);

        // Ensure metadata & timeout survive serialization
        assertEquals(request.getWorkflowId(), requestFromStream.getWorkflowId());
        assertEquals(request.getTemplate().toString(), requestFromStream.getTemplate().toString());
        assertTrue(requestFromStream.isProvision());
        assertFalse(requestFromStream.isUpdateFields());
        assertFalse(requestFromStream.isReprovision());
        assertEquals("bar", requestFromStream.getParams().get("foo"));
        assertEquals(timeout, requestFromStream.getWaitForCompletionTimeout());
        assertEquals(CommonValue.GLOBAL_CONTEXT_INDEX, requestFromStream.index());
        assertEquals("123", requestFromStream.id());
        assertEquals(CommonValue.WORKFLOW_RESOURCE_TYPE, requestFromStream.type());

    }

}
