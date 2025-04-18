/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.opensearch.flowframework.common.WorkflowResources.AGENT_ID;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;

public class ToolStepTests extends OpenSearchTestCase {
    private WorkflowData inputData;
    private WorkflowData inputDataWithAttributes;
    private WorkflowData inputDataWithConnectorId;
    private WorkflowData inputDataWithModelId;
    private WorkflowData inputDataWithAgentId;
    private static final String mockedConnectorId = "mocked-connector-id";
    private static final String mockedModelId = "mocked-model-id";
    private static final String mockedAgentId = "mocked-agent-id";
    private static final String createConnectorNodeId = "create_connector_node_id";
    private static final String createModelNodeId = "create_model_node_id";
    private static final String createAgentNodeId = "create_agent_node_id";

    private WorkflowData boolStringInputData;
    private WorkflowData badBoolInputData;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("type", "type"),
                Map.entry("name", "name"),
                Map.entry("description", "description"),
                Map.entry("parameters", Collections.emptyMap()),
                Map.entry("include_output_in_agent_response", false)
            ),
            "test-id",
            "test-node-id"
        );
        inputDataWithAttributes = new WorkflowData(
            Map.ofEntries(
                Map.entry("type", "type"),
                Map.entry("name", "name"),
                Map.entry("description", "description"),
                Map.entry("parameters", Collections.emptyMap()),
                Map.entry("attributes", Map.of("key1", "value1", "key2", "value2")),
                Map.entry("include_output_in_agent_response", false)
            ),
            "test-id",
            "test-node-id"
        );
        inputDataWithConnectorId = new WorkflowData(Map.of(CONNECTOR_ID, mockedConnectorId), "test-id", createConnectorNodeId);
        inputDataWithModelId = new WorkflowData(Map.of(MODEL_ID, mockedModelId), "test-id", createModelNodeId);
        inputDataWithAgentId = new WorkflowData(Map.of(AGENT_ID, mockedAgentId), "test-id", createAgentNodeId);
        boolStringInputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("type", "type"),
                Map.entry("name", "name"),
                Map.entry("description", "description"),
                Map.entry("parameters", Collections.emptyMap()),
                Map.entry("config", Map.of("foo", "bar")),
                Map.entry("include_output_in_agent_response", "false")
            ),
            "test-id",
            "test-node-id"
        );
        badBoolInputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("type", "type"),
                Map.entry("name", "name"),
                Map.entry("description", "description"),
                Map.entry("parameters", Collections.emptyMap()),
                Map.entry("include_output_in_agent_response", "yes")
            ),
            "test-id",
            "test-node-id"
        );
    }

    public void testTool() throws ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        PlainActionFuture<WorkflowData> future = toolStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );
        assertTrue(future.isDone());
        assertEquals(MLToolSpec.class, future.get().getContent().get("tools").getClass());

        toolStep = new ToolStep();
        future = toolStep.execute(
            boolStringInputData.getNodeId(),
            boolStringInputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );
        assertTrue(future.isDone());
        assertEquals(MLToolSpec.class, future.get().getContent().get("tools").getClass());
        assertEquals(Map.of("foo", "bar"), ((MLToolSpec) future.get().getContent().get("tools")).getConfigMap());
    }

    public void testBoolParseFail() {
        ToolStep toolStep = new ToolStep();

        PlainActionFuture<WorkflowData> future = toolStep.execute(
            badBoolInputData.getNodeId(),
            badBoolInputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        assertTrue(future.isDone());
        ExecutionException e = assertThrows(ExecutionException.class, future::get);
        assertEquals(WorkflowStepException.class, e.getCause().getClass());
        WorkflowStepException w = (WorkflowStepException) e.getCause();
        assertEquals("Failed to parse value [yes] as only [true] or [false] are allowed.", w.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, w.getRestStatus());
    }

    public void testToolWithAttributes() throws ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        PlainActionFuture<WorkflowData> future = toolStep.execute(
            inputDataWithAttributes.getNodeId(),
            inputDataWithAttributes,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );
        assertTrue(future.isDone());
        Object tools = future.get().getContent().get("tools");
        assertEquals(MLToolSpec.class, tools.getClass());
        MLToolSpec mlToolSpec = (MLToolSpec) tools;
        assertEquals(Map.of("key1", "value1", "key2", "value2"), mlToolSpec.getAttributes());
    }

    public void testToolWithConnectorId() throws ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        PlainActionFuture<WorkflowData> future = toolStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of(createConnectorNodeId, inputDataWithConnectorId),
            Map.of(createConnectorNodeId, CONNECTOR_ID),
            Collections.emptyMap(),
            null
        );
        assertTrue(future.isDone());
        Object tools = future.get().getContent().get("tools");
        assertEquals(MLToolSpec.class, tools.getClass());
        MLToolSpec mlToolSpec = (MLToolSpec) tools;
        assertEquals(mlToolSpec.getParameters(), Map.of(CONNECTOR_ID, mockedConnectorId));
    }

    public void testToolWithModelId() throws ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        PlainActionFuture<WorkflowData> future = toolStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of(createModelNodeId, inputDataWithModelId),
            Map.of(createModelNodeId, MODEL_ID),
            Collections.emptyMap(),
            null
        );
        assertTrue(future.isDone());
        Object tools = future.get().getContent().get("tools");
        assertEquals(MLToolSpec.class, tools.getClass());
        MLToolSpec mlToolSpec = (MLToolSpec) tools;
        assertEquals(mlToolSpec.getParameters(), Map.of(MODEL_ID, mockedModelId));
    }

    public void testToolWithAgentId() throws ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        PlainActionFuture<WorkflowData> future = toolStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of(createAgentNodeId, inputDataWithAgentId),
            Map.of(createAgentNodeId, AGENT_ID),
            Collections.emptyMap(),
            null
        );
        assertTrue(future.isDone());
        Object tools = future.get().getContent().get("tools");
        assertEquals(MLToolSpec.class, tools.getClass());
        MLToolSpec mlToolSpec = (MLToolSpec) tools;
        assertEquals(mlToolSpec.getParameters(), Map.of(AGENT_ID, mockedAgentId));
    }
}
